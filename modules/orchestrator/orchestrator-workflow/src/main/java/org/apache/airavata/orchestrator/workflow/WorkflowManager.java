/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.airavata.orchestrator.workflow;

import org.apache.airavata.common.exception.AiravataException;
import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.common.utils.ThriftClientPool;
import org.apache.airavata.common.utils.ThriftUtils;
import org.apache.airavata.helix.core.OutPort;
import org.apache.airavata.helix.core.util.MonitoringUtil;
import org.apache.airavata.helix.workflow.WorkflowOperator;
import org.apache.airavata.messaging.core.*;
import org.apache.airavata.model.messaging.event.WorkflowSubmitEvent;
import org.apache.airavata.model.workflow.*;
import org.apache.airavata.orchestrator.core.exception.OrchestratorException;
import org.apache.airavata.orchestrator.workflow.core.ApplicationTask;
import org.apache.airavata.orchestrator.workflow.core.WorkflowTask;
import org.apache.airavata.orchestrator.workflow.handler.DoWhileLoopTask;
import org.apache.airavata.orchestrator.workflow.handler.FlowStarterTask;
import org.apache.airavata.orchestrator.workflow.handler.FlowTerminatorTask;
import org.apache.airavata.orchestrator.workflow.handler.ForeachLoopTask;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * This is the {@link WorkflowManager} which is responsible for accepting workflow and sending them for execution
 */
public class WorkflowManager {

    private final static Logger logger = LoggerFactory.getLogger(WorkflowManager.class);

    private String workflowManagerName;
    private WorkflowOperator workflowOperator;

    private ThriftClientPool<RegistryService.Client> registryClientPool;

    private Publisher statusPublisher;
    private Subscriber workflowSubscriber;
    private CuratorFramework curatorClient;

    public WorkflowManager() throws ApplicationSettingsException, TException, OrchestratorException {
        this.workflowManagerName = ServerSettings.getSetting("user.workflow.manager.name");
    }

    private void initComponents() throws Exception {
        initRegistryClientPool();
        initWorkflowOperator();
        initStatusPublisher();
        initCurator();

        //start accepting workflow after all initializations are complete
        initWorkflowSubscriber();
    }

    private void initWorkflowOperator() throws Exception {
        workflowOperator = new WorkflowOperator(
                ServerSettings.getSetting("helix.cluster.name"),
                workflowManagerName,
                ServerSettings.getZookeeperConnection());
    }

    private void initStatusPublisher() throws AiravataException {
        this.statusPublisher = MessagingFactory.getPublisher(Type.STATUS);
    }

    private void initCurator() throws ApplicationSettingsException {
        String connectionSting = ServerSettings.getZookeeperConnection();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        curatorClient = CuratorFrameworkFactory.newClient(connectionSting, retryPolicy);
        curatorClient.start();
    }

    private void initRegistryClientPool() throws ApplicationSettingsException {

        GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
        poolConfig.maxActive = 100;
        poolConfig.minIdle = 5;
        poolConfig.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
        poolConfig.testOnBorrow = true;
        poolConfig.testWhileIdle = true;
        poolConfig.numTestsPerEvictionRun = 10;
        poolConfig.maxWait = 3000;

        this.registryClientPool = new ThriftClientPool<>(
                RegistryService.Client::new, poolConfig, ServerSettings.getRegistryServerHost(),
                Integer.parseInt(ServerSettings.getRegistryServerPort()));
    }

    private void initWorkflowSubscriber() throws AiravataException {
        List<String> routingKeys = new ArrayList<>();
        routingKeys.add(ServerSettings.getRabbitmqWorkflowLaunchQueueName());
        this.workflowSubscriber = MessagingFactory.getSubscriber(new WorkflowMessageHandler(), routingKeys, Type.WORKFLOW);
    }

    public static void main(String[] args) throws Exception {
        WorkflowManager workflowManager = new WorkflowManager();
        workflowManager.initComponents();
    }

    public ThriftClientPool<RegistryService.Client> getRegistryClientPool() {
        return registryClientPool;
    }

    public CuratorFramework getCuratorClient() {
        return curatorClient;
    }

    public WorkflowOperator getWorkflowOperator() {
        return workflowOperator;
    }

    public void launchWorkflow(String workflowId, String gatewayId) throws OrchestratorException {
        RegistryService.Client registryClient = getRegistryClientPool().getResource();

        AiravataWorkflow workflow;

        try {
            workflow = registryClient.getWorkflow(workflowId);
            getRegistryClientPool().returnResource(registryClient);

        } catch (TException e) {
            getRegistryClientPool().returnBrokenResource(registryClient);
            throw new OrchestratorException("Failed to fetch workflow from registry associated with workflow id " + workflowId, e);
        }

        List<WorkflowApplication> workflowApps = workflow.getApplications();
        List<WorkflowHandler> workflowHandlers = workflow.getHandlers();
        List<WorkflowConnection> workflowConnections = workflow.getConnections();

        HashMap<String, WorkflowTask> workflowTasks = new HashMap<>();

        for (WorkflowApplication app : workflowApps) {
            WorkflowTask workflowTask = new ApplicationTask();
            workflowTask.setTaskId(app.getId());
            workflowTask.setWorkflowId(workflowId);
            workflowTask.setWorkflowName(workflow.getName());
            workflowTasks.put(app.getId(), workflowTask);
        }

        for (WorkflowHandler handler : workflowHandlers) {
            if (handler.isBelongsToMainWorkflow()) {
                WorkflowTask workflowTask = null;

                if (handler.getType() == HandlerType.DOWHILE_LOOP) {
                    workflowTask = new DoWhileLoopTask();

                } else if (handler.getType() == HandlerType.FOREACH_LOOP) {
                    workflowTask = new ForeachLoopTask();

                } else if (handler.getType() == HandlerType.FLOW_STARTER) {
                    workflowTask = new FlowStarterTask();

                } else if (handler.getType() == HandlerType.FLOW_TERMINATOR) {
                    workflowTask = new FlowTerminatorTask();

                    logger.error("Application type: " + handler.getType() + " is not supported");
                } else {
                    logger.error("Application type: " + handler.getType() + " is not supported");
                }

                if (workflowTask != null) {
                    workflowTask.setTaskId(handler.getId());
                    workflowTask.setWorkflowId(workflowId);
                    workflowTask.setWorkflowName(workflow.getName());
                    workflowTasks.put(handler.getId(), workflowTask);
                }
            }
        }

        for (WorkflowConnection connection : workflowConnections) {
            if (connection.isBelongsToMainWorkflow()) {
                workflowTasks.get(connection.getFromId())
                        .setNextTask(new OutPort(connection.getToId(),
                                workflowTasks.get(connection.getToId())));
            }
        }

        String workflowName;

        try {
            workflowName = getWorkflowOperator().launchWorkflow(workflowId + "-PRE-" + UUID.randomUUID().toString(),
                    new ArrayList<>(workflowTasks.values()), true, false);
        } catch (Exception e) {
            throw new OrchestratorException("Failed to launch workflow with id: " + workflowId, e);
        }

        try {
            MonitoringUtil.registerWorkflow(getCuratorClient(), workflowId, workflowName);
        } catch (Exception e) {
            logger.error("Failed to save workflow " + workflowName + " of id " + workflowId + " in zookeeper registry. " +
                    "This will affect cancellation tasks", e);
        }
    }

    private class WorkflowMessageHandler implements MessageHandler {
        @Override
        public void onMessage(MessageContext messageContext) {
            switch (messageContext.getType()) {
                case WORKFLOW_LAUNCH:
                    try {
                        WorkflowSubmitEvent submitEvent = new WorkflowSubmitEvent();
                        final RegistryService.Client registryClient = getRegistryClientPool().getResource();

                        try {
                            try {
                                byte[] bytes = ThriftUtils.serializeThriftObject(messageContext.getEvent());
                                ThriftUtils.createThriftFromBytes(bytes, submitEvent);
                            } catch (TException e) {
                                throw new OrchestratorException("Workflow launch failed due to Thrift conversion error", e);
                            }

                            logger.info("Launching experiment with experiment id: " + submitEvent.getWorkflowId() +
                                    " gateway id: " + submitEvent.getGatewayId());

                            if (messageContext.isRedeliver()) {
                                AiravataWorkflow workflow;

                                try {
                                    workflow = registryClient.getWorkflow(submitEvent.getWorkflowId());
                                    getRegistryClientPool().returnResource(registryClient);

                                } catch (TException e) {
                                    getRegistryClientPool().returnBrokenResource(registryClient);
                                    throw new OrchestratorException("Failed to fetch workflow from registry associated with workflow id " + submitEvent.getWorkflowId(), e);
                                }

                                if (workflow.getStatuses().get(0).getState() == WorkflowState.CREATED) {
                                    launchWorkflow(submitEvent.getWorkflowId(), submitEvent.getGatewayId());
                                }

                            } else {
                                launchWorkflow(submitEvent.getWorkflowId(), submitEvent.getGatewayId());
                            }

                        } catch (OrchestratorException e) {
                            throw new OrchestratorException("Error occurred while handling workflow launch message with id: " +
                                    messageContext.getMessageId() + " from gateway with id: " + messageContext.getGatewayId(), e);
                        } finally {
                            workflowSubscriber.sendAck(messageContext.getDeliveryTag());
                            getRegistryClientPool().returnResource(registryClient);
                        }

                    } catch (Exception e) {
                        //All exceptions sink here allowing message handler to continue
                        logger.error("Error occurred while handling workflow launch message", e);
                    }
                    break;
                case WORKFLOW_CANCEL:
                    break;
                case WORKFLOW_PAUSE:
                    break;
                case WORKFLOW_RESTART:
                    break;
                default:
                    workflowSubscriber.sendAck(messageContext.getDeliveryTag());
                    logger.error("Orchestrator received an unsupported message type: " + messageContext.getType());
                    break;
            }
        }
    }
}
