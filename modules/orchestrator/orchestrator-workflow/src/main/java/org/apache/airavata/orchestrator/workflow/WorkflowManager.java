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

import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.common.utils.ThriftClientPool;
import org.apache.airavata.helix.core.OutPort;
import org.apache.airavata.helix.core.util.MonitoringUtil;
import org.apache.airavata.helix.workflow.WorkflowOperator;
import org.apache.airavata.model.workflow.*;
import org.apache.airavata.orchestrator.core.exception.OrchestratorException;
import org.apache.airavata.orchestrator.workflow.application.ApplicationTask;
import org.apache.airavata.orchestrator.workflow.core.WorkflowTask;
import org.apache.airavata.orchestrator.workflow.handler.FlowStarterTask;
import org.apache.airavata.orchestrator.workflow.handler.FlowTerminatorTask;
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
    private CuratorFramework curatorClient;

    public WorkflowManager() throws ApplicationSettingsException, TException, OrchestratorException {
        this.workflowManagerName = ServerSettings.getSetting("user.workflow.manager.name");
    }

    public void initComponents() throws Exception {
        initRegistryClientPool();
        initWorkflowOperator();
        initCurator();
    }

    private void initWorkflowOperator() throws Exception {
        workflowOperator = new WorkflowOperator(
                ServerSettings.getSetting("helix.cluster.name"),
                workflowManagerName,
                ServerSettings.getZookeeperConnection());
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

    private ThriftClientPool<RegistryService.Client> getRegistryClientPool() {
        return registryClientPool;
    }

    private CuratorFramework getCuratorClient() {
        return curatorClient;
    }

    private WorkflowOperator getWorkflowOperator() {
        return workflowOperator;
    }

    private void launchWorkflow(String experimentId, String workflowId, String gatewayId) throws OrchestratorException {
        RegistryService.Client registryClient = getRegistryClientPool().getResource();

        AiravataWorkflow workflow;

        try {
            workflow = registryClient.getWorkflow(workflowId);
            getRegistryClientPool().returnResource(registryClient);
        } catch (TException e) {
            getRegistryClientPool().returnBrokenResource(registryClient);
            throw new OrchestratorException("Failed to fetch workflow with id: " + workflowId, e);
        }

        List<WorkflowApplication> workflowApps = workflow.getApplications();
        List<WorkflowHandler> workflowHandlers = workflow.getHandlers();
        List<WorkflowConnection> workflowConnections = workflow.getConnections();

        HashMap<String, WorkflowTask> workflowTasks = new HashMap<>();

        for (WorkflowApplication app : workflowApps) {
            WorkflowTask workflowTask = new ApplicationTask();
            workflowTask.setTaskId(app.getId());
            workflowTask.setExperimentId(experimentId);
            workflowTask.setWorkflowId(workflowId);
            workflowTask.setGatewayId(gatewayId);
            workflowTasks.put(app.getId(), workflowTask);
        }

        for (WorkflowHandler handler : workflowHandlers) {
            WorkflowTask workflowTask = null;

            if (handler.getType() == HandlerType.FLOW_STARTER) {
                workflowTask = new FlowStarterTask();

            } else if (handler.getType() == HandlerType.FLOW_TERMINATOR) {
                workflowTask = new FlowTerminatorTask();
            } else {
                logger.error("Application type: " + handler.getType() + " is not supported in workflow with id: " + workflowId);
            }

            if (workflowTask != null) {
                workflowTask.setTaskId(handler.getId());
                workflowTask.setExperimentId(experimentId);
                workflowTask.setWorkflowId(workflowId);
                workflowTask.setGatewayId(gatewayId);
                workflowTasks.put(handler.getId(), workflowTask);
            }
        }

        for (WorkflowConnection connection : workflowConnections) {
            workflowTasks.get(connection.getFromId())
                    .setNextTask(new OutPort(connection.getToId(), workflowTasks.get(connection.getToId())));
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

    public void launchWorkflowExperiment(String experimentId, String airavataCredStoreToken, String gatewayId)
            throws OrchestratorException {

        RegistryService.Client registryClient = getRegistryClientPool().getResource();
        try {
            String workflowId;
            AiravataWorkflow workflow;

            try {
                workflowId = registryClient.getWorkflowId(experimentId);
                workflow = registryClient.getWorkflow(workflowId);
            } catch (TException e) {
                getRegistryClientPool().returnBrokenResource(registryClient);
                throw new OrchestratorException("Failed to fetch workflow associated with experiment id: " + experimentId, e);
            }

            if (workflow.getStatuses().get(0).getState() == WorkflowState.CREATED) {
                launchWorkflow(experimentId, workflowId, gatewayId);
            }

        } catch (OrchestratorException e) {
            throw new OrchestratorException("Error occurred while launching workflow experiment with id: " +
                    experimentId + " from gateway with id: " + gatewayId, e);
        } finally {
            getRegistryClientPool().returnResource(registryClient);
        }
    }
}
