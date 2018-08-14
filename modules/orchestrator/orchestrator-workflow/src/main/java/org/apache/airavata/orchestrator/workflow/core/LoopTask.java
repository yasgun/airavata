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
import org.apache.airavata.helix.task.api.TaskHelper;
import org.apache.airavata.helix.workflow.QueueOperator;
import org.apache.airavata.model.workflow.WorkflowApplication;
import org.apache.airavata.model.workflow.WorkflowConnection;
import org.apache.airavata.model.workflow.WorkflowHandler;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class LoopTask extends WorkflowTask {

    private final static Logger logger = LoggerFactory.getLogger(LoopTask.class);

    private ThriftClientPool<RegistryService.Client> registryClientPool;

    private QueueOperator queueOperator;

    private List<WorkflowApplication> loopApps;
    private List<WorkflowHandler> loopHandlers;
    private List<WorkflowConnection> loopConnections;

    private WorkflowHandler loopHandler;

    @Override
    public TaskResult onRun(TaskHelper helper) {
        try {
            initRegistryClientPool();
            initQueueOperator();

            RegistryService.Client registryClient = registryClientPool.getResource();

            for (WorkflowHandler handler : registryClient.getWorkflow(getWorkflowId()).getHandlers()) {
                if (handler.getId().equals(getTaskId())) {
                    loopHandler = handler;

                    loopApps = loopHandler.getApplications();
                    loopHandlers = loopHandler.getHandlers();
                    loopConnections = loopHandler.getConnections();
                    break;
                }
            }

            if (loopHandler == null) {
                return onFail("Loop handler not found for loop with id: " + getTaskId(), false);
            }

        } catch (Exception e) {
            logger.error("Loop with id: " + getTaskId() + " on workflow: " + getWorkflowId() + " initialization failed", e);
            return onFail("Loop with id: " + getTaskId() + " on workflow: " + getWorkflowId() + "initialization failed", false);
        }

        return onLoopRun(helper);
    }

    public abstract TaskResult onLoopRun(TaskHelper helper);

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

    private void initQueueOperator() throws Exception {
        queueOperator = new QueueOperator(
                ServerSettings.getSetting("helix.cluster.name"),
                getTaskId(),
                ServerSettings.getZookeeperConnection());
    }

    public WorkflowHandler getLoopHandler() {
        return loopHandler;
    }

    public QueueOperator getQueueOperator() {
        return queueOperator;
    }

    public List<WorkflowApplication> getLoopApps() {
        return loopApps;
    }

    public List<WorkflowHandler> getLoopHandlers() {
        return loopHandlers;
    }

    public List<WorkflowConnection> getLoopConnections() {
        return loopConnections;
    }
}
