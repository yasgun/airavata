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
package org.apache.airavata.orchestrator.workflow.util;

import org.apache.airavata.common.exception.AiravataException;
import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.*;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.impl.CredentialReaderImpl;
import org.apache.airavata.messaging.core.MessageContext;
import org.apache.airavata.messaging.core.Publisher;
import org.apache.airavata.model.messaging.event.ExperimentStatusChangeEvent;
import org.apache.airavata.model.messaging.event.MessageType;
import org.apache.airavata.model.status.ExperimentStatus;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowUtils {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowUtils.class);

    private static ThriftClientPool<RegistryService.Client> registryClientPool;

    public static void updateAndPublishExperimentStatus(String experimentId, ExperimentStatus status, Publisher publisher, String gatewayId) throws TException {
        RegistryService.Client registryClient = null;
        try {
            registryClient = getRegistryClientPool().getResource();
            registryClient.updateExperimentStatus(status, experimentId);
            ExperimentStatusChangeEvent event = new ExperimentStatusChangeEvent(status.getState(), experimentId, gatewayId);
            String messageId = AiravataUtils.getId("EXPERIMENT");
            MessageContext messageContext = new MessageContext(event, MessageType.EXPERIMENT, messageId, gatewayId);
            messageContext.setUpdatedTime(AiravataUtils.getCurrentTimestamp());
            publisher.publish(messageContext);
        } catch (AiravataException e) {
            logger.error("Error while publishing experiment with id: " + experimentId + " status to " + status.toString(), e);
            try {
                WorkflowUtils.getRegistryClientPool().returnBrokenResource(registryClient);
            } catch (ApplicationSettingsException e1) {
                logger.warn("Returning broken resource to the registry client pool failed", e);
            }
        } finally {
            try {
                WorkflowUtils.getRegistryClientPool().returnResource(registryClient);
            } catch (ApplicationSettingsException e) {
                logger.warn("Returning resource to the registry client pool failed", e);
            }
        }
    }

    public static ExperimentStatus getExperimentStatus(String experimentId) throws TException, ApplicationSettingsException {
        RegistryService.Client registryClient = null;
        try {
            registryClient = getRegistryClientPool().getResource();
            return registryClient.getExperimentStatus(experimentId);
        } finally {
            registryClientPool.returnResource(registryClient);
        }
    }

    public static ThriftClientPool<RegistryService.Client> getRegistryClientPool() throws ApplicationSettingsException {

        if (registryClientPool == null) {
            GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
            poolConfig.maxActive = 100;
            poolConfig.minIdle = 5;
            poolConfig.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
            poolConfig.testOnBorrow = true;
            poolConfig.testWhileIdle = true;
            poolConfig.numTestsPerEvictionRun = 10;
            poolConfig.maxWait = 3000;

            registryClientPool = new ThriftClientPool<>(
                    RegistryService.Client::new, poolConfig, ServerSettings.getRegistryServerHost(),
                    Integer.parseInt(ServerSettings.getRegistryServerPort()));
        }

        return registryClientPool;
    }

    public static CredentialReader getCredentialReader()
            throws ApplicationSettingsException, IllegalAccessException,
            InstantiationException {
        try {
            String jdbcUrl = ServerSettings.getCredentialStoreDBURL();
            String jdbcUsr = ServerSettings.getCredentialStoreDBUser();
            String jdbcPass = ServerSettings.getCredentialStoreDBPassword();
            String driver = ServerSettings.getCredentialStoreDBDriver();
            return new CredentialReaderImpl(new DBUtil(jdbcUrl, jdbcUsr, jdbcPass, driver));
        } catch (ClassNotFoundException e) {
            logger.error("Not able to find driver: " + e.getLocalizedMessage());
            return null;
        }
    }
}
