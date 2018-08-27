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
import org.apache.airavata.registry.api.RegistryService;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LoopTask extends WorkflowTask {

    private final static Logger logger = LoggerFactory.getLogger(LoopTask.class);

    private String workflowManagerName;
    private String loopQueueName;

    private ThriftClientPool<RegistryService.Client> registryClientPool;

    private QueueOperator queueOperator;

    private LoopTerminatorTask terminatorTask;

    @Override
    public TaskResult onRun(TaskHelper helper) {
        try {
            this.workflowManagerName = ServerSettings.getSetting("user.workflow.manager.name");

            initRegistryClientPool();
            initQueueOperator();

        } catch (Exception e) {
            logger.error("Loop with id: " + getTaskId() + " on workflow: " + getWorkflowId() + " initialization failed", e);
            return onFail("Loop with id: " + getTaskId() + " on workflow: " + getWorkflowId() + "initialization failed", false);
        }

        loopQueueName = onLoop(helper);

        while (true) {
            try {
                if (isCompleted() == TaskState.COMPLETED) {
                    logger.info("Loop with id: " + getTaskId() + " on workflow with name: " + getWorkflowName() +
                            " and id: " + getWorkflowId() + " completed successfully");
                    break;
                }
            } catch (Exception e) {
                logger.info("Polling for loop completion for loop: " + getTaskId() + " timed out and will continue with polling again.", e);
                try {
                    Thread.sleep(180000L);
                } catch (InterruptedException e1) {
                    // nothing needs to be done
                }
            }
        }

        return onSuccess("Loop with id: " + getTaskId() + " on workflow with name: " + getWorkflowName() + " and id: " +
                getWorkflowId() + " completed");
    }

    /**
     * This method should be implemented based on the requirements of the loop type. This should be returned immediately
     * without waiting for task completions after submitting the tasks.
     *
     * @param helper task helper
     * @return name of the queue where the {@link LoopTerminatorTask} is submitted to be executed at the end of the loop
     */
    public abstract String onLoop(TaskHelper helper);

    public TaskState isCompleted() throws InterruptedException {
        return queueOperator.pollTaskState(loopQueueName, getTerminatorTask().getTaskId(), 180000L, TaskState.COMPLETED);
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

    private void initQueueOperator() throws Exception {
        queueOperator = new QueueOperator(
                ServerSettings.getSetting("helix.cluster.name"),
                workflowManagerName,
                ServerSettings.getZookeeperConnection());
    }

    public LoopTerminatorTask getTerminatorTask() {
        if (terminatorTask == null) {
            terminatorTask = new LoopTerminatorTask();
            terminatorTask.setTaskId(getTaskId() + "-terminator");
        }
        return terminatorTask;
    }

    public QueueOperator getQueueOperator() {
        return queueOperator;
    }

    public ThriftClientPool<RegistryService.Client> getRegistryClientPool() {
        return registryClientPool;
    }
}
