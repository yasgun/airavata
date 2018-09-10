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
import org.apache.airavata.helix.core.participant.HelixParticipant;
import org.apache.airavata.orchestrator.workflow.core.WorkflowTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the {@link HelixParticipant} for user workflow handling
 */
public class WorkflowParticipant extends HelixParticipant<WorkflowTask> {

    private final static Logger logger = LoggerFactory.getLogger(WorkflowParticipant.class);

    private final static String[] taskClassNames = {
            "org.apache.airavata.workflow.core.ApplicationTask",
            "org.apache.airavata.workflow.handler.FlowStarterTask",
            "org.apache.airavata.workflow.handler.FlowTerminatorTask"
    };

    @SuppressWarnings("WeakerAccess")
    public WorkflowParticipant(List<Class<? extends WorkflowTask>> taskClasses, String taskTypeName) throws ApplicationSettingsException {
        super(taskClasses, taskTypeName);
    }

    public static void main(String args[]) {
        logger.info("Starting global participant");

        try {
            ArrayList<Class<? extends WorkflowTask>> taskClasses = new ArrayList<>();

            for (String taskClassName : taskClassNames) {
                taskClasses.add(Class.forName(taskClassName).asSubclass(WorkflowTask.class));
            }

            WorkflowParticipant participant = new WorkflowParticipant(taskClasses, null);

            Thread t = new Thread(participant);
            t.start();

        } catch (Exception e) {
            logger.error("Failed to start global participant", e);
        }
    }

    @Override
    public String getParticipantName() throws ApplicationSettingsException {
        return ServerSettings.getSetting("workflow.participant.name");
    }
}
