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
package org.apache.airavata.orchestrator.workflow.application;

import org.apache.airavata.common.exception.AiravataException;
import org.apache.airavata.common.utils.AiravataUtils;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.messaging.core.MessageContext;
import org.apache.airavata.messaging.core.MessagingFactory;
import org.apache.airavata.messaging.core.Publisher;
import org.apache.airavata.messaging.core.Type;
import org.apache.airavata.model.messaging.event.MessageType;
import org.apache.airavata.model.messaging.event.ProcessSubmitEvent;
import org.apache.airavata.model.messaging.event.ProcessTerminateEvent;
import org.apache.airavata.orchestrator.core.exception.OrchestratorException;
import org.apache.airavata.orchestrator.workflow.core.JobSubmitter;
import org.apache.airavata.orchestrator.workflow.util.WorkflowUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ApplicationJobSubmitter implements JobSubmitter, Watcher {
    private final static Logger logger = LoggerFactory.getLogger(ApplicationJobSubmitter.class);

    private static Integer mutex = -1;

    private Publisher publisher;

    public void initialize() throws OrchestratorException {
        try {
            this.publisher = MessagingFactory.getPublisher(Type.PROCESS_LAUNCH);
        } catch (AiravataException e) {
            logger.error(e.getMessage(), e);
            throw new OrchestratorException("Cannot initialize " + ApplicationJobSubmitter.class +
                    " need to start Rabbitmq server to use", e);
        }
    }

    public void submit(String experimentId, String processId, String tokenId) throws OrchestratorException {
        try {
            String gatewayId = null;
            CredentialReader credentialReader = WorkflowUtils.getCredentialReader();

            if (credentialReader != null) {
                try {
                    gatewayId = credentialReader.getGatewayID(tokenId);
                } catch (Exception e) {
                    logger.warn("Error while extracting gateway id for provided token", e);
                }
            }

            if (gatewayId == null || gatewayId.isEmpty()) {
                gatewayId = ServerSettings.getDefaultUserGateway();
            }

            ProcessSubmitEvent processSubmitEvent = new ProcessSubmitEvent(processId, gatewayId, experimentId, tokenId);
            MessageContext messageContext = new MessageContext(processSubmitEvent, MessageType.LAUNCHPROCESS,
                    "LAUNCH.PROCESS-" + UUID.randomUUID().toString(), gatewayId);

            messageContext.setUpdatedTime(AiravataUtils.getCurrentTimestamp());
            publisher.publish(messageContext);
        } catch (Exception e) {
            logger.error("Error while submitting process with id: " + processId + "of experiment with id: " + experimentId, e);
            throw new OrchestratorException("Error while submitting process with id: " + processId + "of experiment with id: " + experimentId, e);
        }
    }

    public void terminate(String experimentId, String processId, String tokenId) throws OrchestratorException {
        try {
            String gatewayId = null;

            CredentialReader credentialReader = WorkflowUtils.getCredentialReader();

            if (credentialReader != null) {
                try {
                    gatewayId = credentialReader.getGatewayID(tokenId);
                } catch (Exception e) {
                    logger.warn("Error while extracting gateway id for provided token", e);
                }
            }

            if (gatewayId == null || gatewayId.isEmpty()) {
                gatewayId = ServerSettings.getDefaultUserGateway();
            }

            ProcessTerminateEvent processTerminateEvent = new ProcessTerminateEvent(processId, gatewayId, tokenId);
            MessageContext messageContext = new MessageContext(processTerminateEvent, MessageType.TERMINATEPROCESS,
                    "LAUNCH.TERMINATE-" + UUID.randomUUID().toString(), gatewayId);

            messageContext.setUpdatedTime(AiravataUtils.getCurrentTimestamp());
            publisher.publish(messageContext);
        } catch (Exception e) {
            logger.error("Error while terminating process with id: " + processId + "of experiment with id: " + experimentId, e);
            throw new OrchestratorException("Error while terminating process with id: " + processId + "of experiment with id: " + experimentId, e);
        }
    }

    //TODO Clarify
    synchronized public void process(WatchedEvent event) {
        logger.info(getClass().getName() + event.getPath());
        logger.info(getClass().getName() + event.getType());
        synchronized (mutex) {
            switch (event.getState()) {
                case SyncConnected:
                    mutex.notify();
            }
            switch (event.getType()) {
                case NodeCreated:
                    mutex.notify();
                    break;
            }
        }
    }
}
