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

import org.apache.airavata.helix.core.AbstractTask;
import org.apache.airavata.helix.task.api.annotation.TaskParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WorkflowTask extends AbstractTask {

    private final static Logger logger = LoggerFactory.getLogger(WorkflowTask.class);

    @TaskParam(name = "workflowId")
    private String workflowId;

    @TaskParam(name = "gatewayId")
    private String gatewayId;

    @TaskParam(name = "experimentId")
    private String experimentId;


    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public void setGatewayId(String gatewayId) {
        this.gatewayId = gatewayId;
    }

    public void setExperimentId(String experimentId) {
        this.experimentId = experimentId;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public String getGatewayId() {
        return gatewayId;
    }

    public String getExperimentId() {
        return experimentId;
    }
}
