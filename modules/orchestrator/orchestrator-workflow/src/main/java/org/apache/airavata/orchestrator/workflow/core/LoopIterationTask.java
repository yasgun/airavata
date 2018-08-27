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
package org.apache.airavata.orchestrator.workflow.core;

import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.common.utils.ThriftClientPool;
import org.apache.airavata.helix.core.OutPort;
import org.apache.airavata.helix.core.util.MonitoringUtil;
import org.apache.airavata.helix.task.api.TaskHelper;
import org.apache.airavata.helix.task.api.annotation.TaskDef;
import org.apache.airavata.helix.task.api.annotation.TaskParam;
import org.apache.airavata.helix.workflow.WorkflowOperator;
import org.apache.airavata.model.workflow.HandlerType;
import org.apache.airavata.model.workflow.WorkflowApplication;
import org.apache.airavata.model.workflow.WorkflowConnection;
import org.apache.airavata.model.workflow.WorkflowHandler;
import org.apache.airavata.orchestrator.workflow.handler.DoWhileLoopTask;
import org.apache.airavata.orchestrator.workflow.handler.FlowStarterTask;
import org.apache.airavata.orchestrator.workflow.handler.FlowTerminatorTask;
import org.apache.airavata.orchestrator.workflow.handler.ForeachLoopTask;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

@TaskDef(name = "Loop Iteration Task")
public class LoopIterationTask extends WorkflowTask {
    private final static Logger logger = LoggerFactory.getLogger(LoopIterationTask.class);

    private WorkflowOperator workflowOperator;

    private String workflowManagerName;
    private String workflowName;
    private WorkflowHandler handler;
    private HashMap<String, WorkflowTask> workflowTasks;

    private ThriftClientPool<RegistryService.Client> registryClientPool;

    @TaskParam(name = "loopId")
    private String loopId;

    @Override
    public TaskResult onRun(TaskHelper helper) {
        try {
            this.workflowManagerName = ServerSettings.getSetting("user.workflow.manager.name");

            initRegistryClientPool();
            initWorkflowOperator();

            RegistryService.Client registryClient = registryClientPool.getResource();

            for (WorkflowHandler loopHandler : registryClient.getWorkflow(getWorkflowId()).getHandlers()) {
                if (loopHandler.getId().equals(getLoopId())) {
                    handler = loopHandler;

                    workflowTasks = new HashMap<>();

                    for (WorkflowApplication app : handler.getApplications()) {
                        WorkflowTask workflowTask = new ApplicationTask();
                        workflowTask.setTaskId(app.getId());
                        workflowTask.setWorkflowId(getWorkflowId());
                        workflowTask.setWorkflowName(getWorkflowName());
                        workflowTasks.put(app.getId(), workflowTask);
                    }

                    for (WorkflowHandler handler : handler.getHandlers()) {
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
                                workflowTask.setWorkflowId(getWorkflowId());
                                workflowTask.setWorkflowName(getWorkflowName());
                                workflowTasks.put(handler.getId(), workflowTask);
                            }
                        }
                    }

                    for (WorkflowConnection connection : loopHandler.getConnections()) {
                        if (connection.isBelongsToMainWorkflow()) {
                            workflowTasks.get(connection.getFromId())
                                    .setNextTask(new OutPort(connection.getToId(),
                                            workflowTasks.get(connection.getToId())));
                        }
                    }
                    break;
                }
            }

            if (handler == null) {
                return onFail("Loop handler not found for loop with id: " + getTaskId(), false);
            }

        } catch (Exception e) {
            logger.error("Loop with id: " + getTaskId() + " on workflow: " + getWorkflowId() + " initialization failed", e);
            return onFail("Loop with id: " + getTaskId() + " on workflow: " + getWorkflowId() + "initialization failed", false);
        }

        try {
            workflowName = getWorkflowOperator().launchWorkflow(getTaskId() + "-ITERATION-" + UUID.randomUUID().toString(),
                    new ArrayList<>(workflowTasks.values()), true, false);
        } catch (Exception e) {
            return onFail("Failed to launch workflow with id: " + getTaskId(), false);
        }

        try {
            MonitoringUtil.registerWorkflow(getCuratorClient(), getTaskId(), workflowName);
        } catch (Exception e) {
            logger.error("Failed to save workflow " + workflowName + " of id " + getTaskId() + " in zookeeper registry. " +
                    "This will affect cancellation tasks", e);
        }

        while (true) {
            try {
                if (isCompleted() == TaskState.COMPLETED) {
                    logger.info("Loop iteration with id: " + getTaskId() + " on workflow with name: " + getWorkflowName() +
                            " and id: " + getWorkflowId() + " completed successfully");
                    break;
                }
            } catch (Exception e) {
                logger.info("Polling for loop iteration completion for loop: " + getTaskId() + " timed out and will continue with polling again.", e);
                try {
                    Thread.sleep(180000L);
                } catch (InterruptedException e1) {
                    // nothing needs to be done
                }
            }
        }

        return onSuccess("Loop iteration with id: " + getTaskId() + " on workflow with name: " + getWorkflowName() + " and id: " +
                getWorkflowId() + " completed");
    }

    @Override
    public void onCancel() {

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

    private void initWorkflowOperator() throws Exception {
        workflowOperator = new WorkflowOperator(
                ServerSettings.getSetting("helix.cluster.name"),
                workflowManagerName,
                ServerSettings.getZookeeperConnection());
    }

    public WorkflowOperator getWorkflowOperator() {
        return workflowOperator;
    }

    public TaskState isCompleted() throws InterruptedException {
        return workflowOperator.pollWorkflowState(workflowName, 180000L, TaskState.COMPLETED);
    }

    public void setLoopId(String loopId) {
        this.loopId = loopId;
    }

    public String getLoopId() {
        return loopId;
    }
}
