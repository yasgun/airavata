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
import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.logging.MDCUtil;
import org.apache.airavata.common.utils.*;
import org.apache.airavata.gfac.core.GFacConstants;
import org.apache.airavata.gfac.core.GFacUtils;
import org.apache.airavata.gfac.core.scheduler.HostScheduler;
import org.apache.airavata.gfac.core.task.TaskException;
import org.apache.airavata.helix.task.api.TaskHelper;
import org.apache.airavata.helix.task.api.annotation.TaskDef;
import org.apache.airavata.messaging.core.*;
import org.apache.airavata.model.appcatalog.appdeployment.ApplicationDeploymentDescription;
import org.apache.airavata.model.appcatalog.appinterface.ApplicationInterfaceDescription;
import org.apache.airavata.model.appcatalog.computeresource.*;
import org.apache.airavata.model.appcatalog.gatewayprofile.ComputeResourcePreference;
import org.apache.airavata.model.appcatalog.gatewayprofile.GatewayResourceProfile;
import org.apache.airavata.model.application.io.DataType;
import org.apache.airavata.model.application.io.InputDataObjectType;
import org.apache.airavata.model.application.io.OutputDataObjectType;
import org.apache.airavata.model.commons.ErrorModel;
import org.apache.airavata.model.data.movement.DataMovementProtocol;
import org.apache.airavata.model.data.replica.DataProductModel;
import org.apache.airavata.model.data.replica.DataReplicaLocationModel;
import org.apache.airavata.model.data.replica.ReplicaLocationCategory;
import org.apache.airavata.model.error.LaunchValidationException;
import org.apache.airavata.model.experiment.ExperimentModel;
import org.apache.airavata.model.experiment.UserConfigurationDataModel;
import org.apache.airavata.model.messaging.event.*;
import org.apache.airavata.model.process.ProcessModel;
import org.apache.airavata.model.scheduling.ComputationalResourceSchedulingModel;
import org.apache.airavata.model.status.ExperimentState;
import org.apache.airavata.model.status.ExperimentStatus;
import org.apache.airavata.model.status.TaskState;
import org.apache.airavata.model.status.TaskStatus;
import org.apache.airavata.model.task.*;
import org.apache.airavata.model.workflow.WorkflowApplication;
import org.apache.airavata.orchestrator.core.exception.OrchestratorException;
import org.apache.airavata.orchestrator.util.OrchestratorServerThreadPoolExecutor;
import org.apache.airavata.orchestrator.util.OrchestratorUtils;
import org.apache.airavata.orchestrator.workflow.core.JobSubmitter;
import org.apache.airavata.orchestrator.workflow.core.WorkflowTask;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.airavata.registry.api.client.RegistryServiceClientFactory;
import org.apache.airavata.registry.api.exception.RegistryServiceException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.helix.task.TaskResult;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

@TaskDef(name = "Application Processor Task")
public class ApplicationTask extends WorkflowTask {

    private static Logger logger = LoggerFactory.getLogger(ApplicationTask.class);

    private Publisher statusPublisher;
    private CuratorFramework curatorClient;
    private JobSubmitter jobSubmitter;

    private ThriftClientPool<RegistryService.Client> registryClientPool;
    private WorkflowApplication workflowApplication;

    @Override
    public TaskResult onRun(TaskHelper helper) {
        try {
            initialize();

            RegistryService.Client registryClient = registryClientPool.getResource();

            for (WorkflowApplication application : registryClient.getWorkflow(getWorkflowId()).getApplications()) {
                if (application.getId().equals(getTaskId())) {
                    workflowApplication = application;
                    break;
                }
            }
        } catch (Exception e) {
            //
        }

        return onSuccess("Application with id: " + getTaskId() + " on workflow with id: " + getWorkflowId() + " and id: " +
                getWorkflowId() + " completed");
    }

    @Override
    public void onCancel() {

    }

    private void initialize() throws AiravataException, OrchestratorException {
        statusPublisher = MessagingFactory.getPublisher(Type.STATUS);
        startCurator();
        jobSubmitter = new ApplicationJobSubmitter();
        jobSubmitter.initialize();
        initRegistryClientPool();
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

    /**
     * * After creating the experiment Data user have the * experimentID as the
     * handler to the experiment, during the launchProcess * We just have to
     * give the experimentID * * @param experimentID * @return sucess/failure *
     * *
     *
     * @param experimentId
     */
    public boolean launchApplication(String experimentId, String gatewayId) throws TException {
        final RegistryService.Client registryClient = getRegistryServiceClient();
        try {
            String experimentNodePath = GFacUtils.getExperimentNodePath(experimentId);
            ZKPaths.mkdirs(curatorClient.getZookeeperClient().getZooKeeper(), experimentNodePath);
            String experimentCancelNode = ZKPaths.makePath(experimentNodePath, ZkConstants.ZOOKEEPER_CANCEL_LISTENER_NODE);
            ZKPaths.mkdirs(curatorClient.getZookeeperClient().getZooKeeper(), experimentCancelNode);

            ComputeResourcePreference computeResourcePreference = registryClient.getGatewayComputeResourcePreference
                    (gatewayId, workflowApplication.getComputeResourceId());
            String token = computeResourcePreference.getResourceSpecificCredentialStoreToken();

            if (token == null || token.isEmpty()) {
                // try with gateway profile level token
                GatewayResourceProfile gatewayProfile = registryClient.getGatewayResourceProfile(gatewayId);
                token = gatewayProfile.getCredentialStoreToken();
            }
            // still the token is empty, then we fail the experiment
            if (token == null || token.isEmpty()) {
                logger.error("You have not configured credential store token at gateway profile or compute resource preference." +
                        " Please provide the correct token at gateway profile or compute resource preference.");
                return false;
            }
            List<ProcessModel> processes = createProcesses(experimentId, gatewayId);

            for (ProcessModel processModel : processes) {
                //FIXME Resolving replica if available. This is a very crude way of resolving input replicas. A full featured
                //FIXME replica resolving logic should come here
                processModel.getProcessInputs().stream().forEach(pi -> {
                    if (pi.getType().equals(DataType.URI) && pi.getValue().startsWith("airavata-dp://")) {
                        try {
                            DataProductModel dataProductModel = registryClient.getDataProduct(pi.getValue());
                            Optional<DataReplicaLocationModel> rpLocation = dataProductModel.getReplicaLocations()
                                    .stream().filter(rpModel -> rpModel.getReplicaLocationCategory().
                                            equals(ReplicaLocationCategory.GATEWAY_DATA_STORE)).findFirst();
                            if (rpLocation.isPresent()) {
                                pi.setValue(rpLocation.get().getFilePath());
                                pi.setStorageResourceId(rpLocation.get().getStorageResourceId());
                            } else {
                                logger.error("Could not find a replica for the URI " + pi.getValue());
                            }
                        } catch (RegistryServiceException e) {
                            throw new RuntimeException("Error while launching experiment", e);
                        } catch (TException e) {
                            throw new RuntimeException("Error while launching experiment", e);
                        }
                    } else if (pi.getType().equals(DataType.URI_COLLECTION) && pi.getValue().contains("airavata-dp://")) {
                        try {
                            String[] uriList = pi.getValue().split(",");
                            final ArrayList<String> filePathList = new ArrayList<>();
                            for (String uri : uriList) {
                                if (uri.startsWith("airavata-dp://")) {
                                    DataProductModel dataProductModel = registryClient.getDataProduct(uri);
                                    Optional<DataReplicaLocationModel> rpLocation = dataProductModel.getReplicaLocations()
                                            .stream().filter(rpModel -> rpModel.getReplicaLocationCategory().
                                                    equals(ReplicaLocationCategory.GATEWAY_DATA_STORE)).findFirst();
                                    if (rpLocation.isPresent()) {
                                        filePathList.add(rpLocation.get().getFilePath());
                                    } else {
                                        logger.error("Could not find a replica for the URI " + pi.getValue());
                                    }
                                } else {
                                    // uri is in file path format
                                    filePathList.add(uri);
                                }
                            }
                            pi.setValue(StringUtils.join(filePathList, ','));
                        } catch (RegistryServiceException e) {
                            throw new RuntimeException("Error while launching experiment", e);
                        } catch (TException e) {
                            throw new RuntimeException("Error while launching experiment", e);
                        }
                    }
                });
                String taskDag = createAndSaveTasks(gatewayId, processModel, false);
                processModel.setTaskDag(taskDag);
                registryClient.updateProcess(processModel, processModel.getProcessId());


                if (!validateProcess(experimentId, processes)) {
                    logger.error("Validating process fails for given experiment Id : {}", experimentId);
                    return false;
                }

                logger.debug(experimentId, "Launching single application experiment {}.", experimentId);
                ExperimentStatus status = new ExperimentStatus(ExperimentState.LAUNCHED);
                status.setReason("submitted all processes");
                status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
                OrchestratorUtils.updateAndPublishExperimentStatus(experimentId, status, statusPublisher, gatewayId);
                logger.info("expId: {}, Launched experiment ", experimentId);
                OrchestratorServerThreadPoolExecutor.getCachedThreadPool().execute(MDCUtil.wrapWithMDC(new SingleAppExperimentRunner(experimentId, token, gatewayId)));
            }
        } catch (LaunchValidationException launchValidationException) {
            ExperimentStatus status = new ExperimentStatus(ExperimentState.FAILED);
            status.setReason("Validation failed: " + launchValidationException.getErrorMessage());
            status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
            OrchestratorUtils.updateAndPublishExperimentStatus(experimentId, status, statusPublisher, gatewayId);
            throw new TException("Experiment '" + experimentId + "' launch failed. Experiment failed to validate: " + launchValidationException.getErrorMessage(), launchValidationException);
        } catch (Exception e) {
            throw new TException("Experiment '" + experimentId + "' launch failed. Unable to figureout execution type for application " + getTaskId(), e);
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
        return true;
    }

    public boolean validateProcess(String experimentId, List<ProcessModel> processes) throws LaunchValidationException, TException {
        final RegistryService.Client registryClient = getRegistryServiceClient();
        try {
            ExperimentModel experimentModel = registryClient.getExperiment(experimentId);
//            for (ProcessModel processModel : processes) {
//                boolean state = validateProcess(experimentModel, processModel).isSetValidationState();
//                if (!state) {
//                    return false;
//                }
//            }
            return true;
        } catch (LaunchValidationException lve) {

            // If a process failed to validate, also add an error message at the experiment level
            ErrorModel details = new ErrorModel();
            details.setActualErrorMessage(lve.getErrorMessage());
            details.setCreationTime(Calendar.getInstance().getTimeInMillis());
            registryClient.addErrors(GFacConstants.EXPERIMENT_ERROR, details, experimentId);
            throw lve;
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
    }

    /**
     * This can be used to cancel a running experiment and store the status to
     * terminated in registry
     *
     * @param experimentId
     * @return
     * @throws TException
     */
    public boolean terminateExperiment(String experimentId, String gatewayId) throws TException {
        final RegistryService.Client registryClient = getRegistryServiceClient();
        logger.info(experimentId, "Experiment: {} is cancelling  !!!!!", experimentId);
        try {
            return validateStatesAndCancel(registryClient, experimentId, gatewayId);
        } catch (Exception e) {
            logger.error("expId : " + experimentId + " :- Error while cancelling experiment", e);
            return false;
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
    }

    public boolean launchProcess(String processId, String airavataCredStoreToken, String gatewayId) throws TException {
        final RegistryService.Client registryClient = getRegistryServiceClient();
        try {
            ProcessModel processModel = registryClient.getProcess(processId);
            String applicationId = processModel.getApplicationInterfaceId();
            if (applicationId == null) {
                logger.error(processId, "Application interface id shouldn't be null.");
                throw new OrchestratorException("Error executing the job, application interface id shouldn't be null.");
            }
            // set application deployment id to process model
            ApplicationDeploymentDescription applicationDeploymentDescription = getAppDeployment(registryClient, processModel, applicationId);
            processModel.setApplicationDeploymentId(applicationDeploymentDescription.getAppDeploymentId());
            // set compute resource id to process model, default we set the same in the user preferred compute host id
            processModel.setComputeResourceId(processModel.getProcessResourceSchedule().getResourceHostId());
            registryClient.updateProcess(processModel, processModel.getProcessId());
            return launchProcess(processModel, airavataCredStoreToken);
        } catch (Exception e) {
            logger.error(processId, "Error while launching process ", e);
            throw new TException(e);
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
    }

    public boolean launchProcess(ProcessModel processModel, String tokenId) throws OrchestratorException {
        try {
            return jobSubmitter.submit(processModel.getExperimentId(), processModel.getProcessId(), tokenId);
        } catch (Exception e) {
            throw new OrchestratorException("Error launching the job", e);
        }
    }

    private ApplicationDeploymentDescription getAppDeployment(RegistryService.Client registryClient, ProcessModel processModel, String applicationId)
            throws OrchestratorException,
            ClassNotFoundException, ApplicationSettingsException,
            InstantiationException, IllegalAccessException, TException {
        String selectedModuleId = getModuleId(registryClient, applicationId);
        return getAppDeploymentForModule(registryClient, processModel, selectedModuleId);
    }

    private ApplicationDeploymentDescription getAppDeploymentForModule(RegistryService.Client registryClient, ProcessModel processModel, String selectedModuleId)
            throws ClassNotFoundException,
            ApplicationSettingsException, InstantiationException,
            IllegalAccessException, TException {

        List<ApplicationDeploymentDescription> applicationDeployements = registryClient.getApplicationDeployments(selectedModuleId);
        Map<ComputeResourceDescription, ApplicationDeploymentDescription> deploymentMap = new HashMap<ComputeResourceDescription, ApplicationDeploymentDescription>();

        for (ApplicationDeploymentDescription deploymentDescription : applicationDeployements) {
            if (processModel.getComputeResourceId().equals(deploymentDescription.getComputeHostId())) {
                deploymentMap.put(registryClient.getComputeResource(deploymentDescription.getComputeHostId()), deploymentDescription);
            }
        }
        List<ComputeResourceDescription> computeHostList = Arrays.asList(deploymentMap.keySet().toArray(new ComputeResourceDescription[]{}));
        Class<? extends HostScheduler> aClass = Class.forName(
                ServerSettings.getHostScheduler()).asSubclass(
                HostScheduler.class);
        HostScheduler hostScheduler = aClass.newInstance();
        ComputeResourceDescription ComputeResourceDescription = hostScheduler.schedule(computeHostList);
        return deploymentMap.get(ComputeResourceDescription);
    }

    private String getModuleId(RegistryService.Client registryClient, String applicationId)
            throws OrchestratorException, TException {
        ApplicationInterfaceDescription applicationInterface = registryClient.getApplicationInterface(applicationId);
        List<String> applicationModules = applicationInterface.getApplicationModules();
        if (applicationModules.size() == 0) {
            throw new OrchestratorException(
                    "No modules defined for application "
                            + applicationId);
        }
//			AiravataAPI airavataAPI = getAiravataAPI();
        String selectedModuleId = applicationModules.get(0);
        return selectedModuleId;
    }

    private boolean validateStatesAndCancel(RegistryService.Client registryClient, String experimentId, String gatewayId) throws Exception {
        ExperimentStatus experimentStatus = registryClient.getExperimentStatus(experimentId);
        switch (experimentStatus.getState()) {
            case COMPLETED:
            case CANCELED:
            case FAILED:
            case CANCELING:
                logger.warn("Can't terminate already {} experiment", experimentStatus.getState().name());
                return false;
            case CREATED:
                logger.warn("Experiment termination is only allowed for launched experiments.");
                return false;
            default:
                ExperimentModel experimentModel = registryClient.getExperiment(experimentId);

                ComputeResourcePreference computeResourcePreference = registryClient.getGatewayComputeResourcePreference
                        (gatewayId,
                                experimentModel.getUserConfigurationData().getComputationalResourceScheduling().getResourceHostId());
                String token = computeResourcePreference.getResourceSpecificCredentialStoreToken();
                if (token == null || token.isEmpty()) {
                    // try with gateway profile level token
                    GatewayResourceProfile gatewayProfile = registryClient.getGatewayResourceProfile(gatewayId);
                    token = gatewayProfile.getCredentialStoreToken();
                }
                // still the token is empty, then we fail the experiment
                if (token == null || token.isEmpty()) {
                    logger.error("You have not configured credential store token at gateway profile or compute resource preference." +
                            " Please provide the correct token at gateway profile or compute resource preference.");
                    return false;
                }

                cancelExperiment(experimentModel, token);
                // TODO deprecate this approach as we are replacing gfac
                String expCancelNodePath = ZKPaths.makePath(ZKPaths.makePath(ZkConstants.ZOOKEEPER_EXPERIMENT_NODE,
                        experimentId), ZkConstants.ZOOKEEPER_CANCEL_LISTENER_NODE);
                Stat stat = curatorClient.checkExists().forPath(expCancelNodePath);
                if (stat != null) {
                    curatorClient.setData().withVersion(-1).forPath(expCancelNodePath, ZkConstants.ZOOKEEPER_CANCEL_REQEUST
                            .getBytes());
                    ExperimentStatus status = new ExperimentStatus(ExperimentState.CANCELING);
                    status.setReason("Experiment cancel request processed");
                    status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
                    OrchestratorUtils.updateAndPublishExperimentStatus(experimentId, status, statusPublisher, gatewayId);
                    logger.info("expId : " + experimentId + " :- Experiment status updated to " + status.getState());
                    return true;
                }
                return false;
        }
    }

    public void cancelExperiment(ExperimentModel experiment, String tokenId) throws OrchestratorException {
        logger.info("Terminating experiment " + experiment.getExperimentId());
        RegistryService.Client registryServiceClient = getRegistryServiceClient();

        try {
            List<String> processIds = registryServiceClient.getProcessIds(experiment.getExperimentId());
            if (processIds != null && processIds.size() > 0) {
                for (String processId : processIds) {
                    logger.info("Terminating process " + processId + " of experiment " + experiment.getExperimentId());
                    jobSubmitter.terminate(experiment.getExperimentId(), processId, tokenId);
                }
            } else {
                logger.warn("No processes found for experiment " + experiment.getExperimentId() + " to cancel");
            }
        } catch (TException e) {
            logger.error("Failed to fetch process ids for experiment " + experiment.getExperimentId(), e);
            throw new OrchestratorException("Failed to fetch process ids for experiment " + experiment.getExperimentId(), e);
        }
    }

    private void startCurator() throws ApplicationSettingsException {
        String connectionSting = ServerSettings.getZookeeperConnection();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        curatorClient = CuratorFrameworkFactory.newClient(connectionSting, retryPolicy);
        curatorClient.start();
    }

    private class SingleAppExperimentRunner implements Runnable {

        String experimentId;
        String airavataCredStoreToken;
        String gatewayId;

        public SingleAppExperimentRunner(String experimentId, String airavataCredStoreToken, String gatewayId) {
            this.experimentId = experimentId;
            this.airavataCredStoreToken = airavataCredStoreToken;
            this.gatewayId = gatewayId;
        }

        @Override
        public void run() {
            try {
                launchSingleAppExperiment();
            } catch (TException e) {
                logger.error("Unable to launch experiment..", e);
                throw new RuntimeException("Error while launching experiment", e);
            } catch (AiravataException e) {
                logger.error("Unable to publish experiment status..", e);
            }
        }

        private boolean launchSingleAppExperiment() throws TException, AiravataException {
            final RegistryService.Client registryClient = getRegistryServiceClient();
            try {
                List<String> processIds = registryClient.getProcessIds(experimentId);
                for (String processId : processIds) {
                    launchProcess(processId, airavataCredStoreToken, gatewayId);
                }
//				ExperimentStatus status = new ExperimentStatus(ExperimentState.LAUNCHED);
//				status.setReason("submitted all processes");
//				status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
//				OrchestratorUtils.updageAndPublishExperimentStatus(experimentId, status);
//				logger.info("expId: {}, Launched experiment ", experimentId);
            } catch (Exception e) {
                ExperimentStatus status = new ExperimentStatus(ExperimentState.FAILED);
                status.setReason("Error while updating task status");
                OrchestratorUtils.updateAndPublishExperimentStatus(experimentId, status, statusPublisher, gatewayId);
                logger.error("expId: " + experimentId + ", Error while updating task status, hence updated experiment status to " +
                        ExperimentState.FAILED, e);
                ExperimentStatusChangeEvent event = new ExperimentStatusChangeEvent(ExperimentState.FAILED,
                        experimentId,
                        gatewayId);
                String messageId = AiravataUtils.getId("EXPERIMENT");
                MessageContext messageContext = new MessageContext(event, MessageType.EXPERIMENT, messageId, gatewayId);
                messageContext.setUpdatedTime(AiravataUtils.getCurrentTimestamp());
                statusPublisher.publish(messageContext);
                throw new TException(e);
            } finally {
                if (registryClient != null) {
                    ThriftUtils.close(registryClient);
                }
            }
            return true;
        }

    }


    private class ProcessStatusHandler implements MessageHandler {
        /**
         * This method only handle MessageType.PROCESS type messages.
         *
         * @param message
         */
        @Override
        public void onMessage(MessageContext message) {
            if (message.getType().equals(MessageType.PROCESS)) {
                try {
                    ProcessStatusChangeEvent processStatusChangeEvent = new ProcessStatusChangeEvent();
                    TBase event = message.getEvent();
                    byte[] bytes = ThriftUtils.serializeThriftObject(event);
                    ThriftUtils.createThriftFromBytes(bytes, processStatusChangeEvent);
                    ExperimentStatus status = new ExperimentStatus();
                    ProcessIdentifier processIdentity = processStatusChangeEvent.getProcessIdentity();
                    logger.info("expId: {}, processId: {} :- Process status changed event received for status {}",
                            processIdentity.getExperimentId(), processIdentity.getProcessId(),
                            processStatusChangeEvent.getState().name());
                    switch (processStatusChangeEvent.getState()) {
//						case CREATED:
//						case VALIDATED:
                        case STARTED:
                            try {
                                ExperimentStatus stat = OrchestratorUtils.getExperimentStatus(processIdentity
                                        .getExperimentId());
                                if (stat.getState() == ExperimentState.CANCELING) {
                                    status.setState(ExperimentState.CANCELING);
                                    status.setReason("Process started but experiment cancelling is triggered");
                                } else {
                                    status.setState(ExperimentState.EXECUTING);
                                    status.setReason("process  started");
                                }
                            } catch (ApplicationSettingsException e) {
                                throw new RuntimeException("Error ", e);
                            }
                            break;
//						case PRE_PROCESSING:
//							break;
//						case CONFIGURING_WORKSPACE:
//						case INPUT_DATA_STAGING:
//						case EXECUTING:
//						case MONITORING:
//						case OUTPUT_DATA_STAGING:
//						case POST_PROCESSING:
//						case CANCELLING:
//							break;
                        case COMPLETED:
                            try {
                                ExperimentStatus stat = OrchestratorUtils.getExperimentStatus(processIdentity
                                        .getExperimentId());
                                if (stat.getState() == ExperimentState.CANCELING) {
                                    status.setState(ExperimentState.CANCELED);
                                    status.setReason("Process competed but experiment cancelling is triggered");
                                } else {
                                    status.setState(ExperimentState.COMPLETED);
                                    status.setReason("process  completed");
                                }
                            } catch (ApplicationSettingsException e) {
                                throw new RuntimeException("Error ", e);
                            }
                            break;
                        case FAILED:
                            try {
                                ExperimentStatus stat = OrchestratorUtils.getExperimentStatus(processIdentity
                                        .getExperimentId());
                                if (stat.getState() == ExperimentState.CANCELING) {
                                    status.setState(ExperimentState.CANCELED);
                                    status.setReason("Process failed but experiment cancelling is triggered");
                                } else {
                                    status.setState(ExperimentState.FAILED);
                                    status.setReason("process  failed");
                                }
                            } catch (ApplicationSettingsException e) {
                                throw new RuntimeException("Unable to create registry client...", e);
                            }
                            break;
                        case CANCELED:
                            // TODO if experiment have more than one process associated with it, then this should be changed.
                            status.setState(ExperimentState.CANCELED);
                            status.setReason("process  cancelled");
                            break;
                        default:
                            // ignore other status changes, thoes will not affect for experiment status changes
                            return;
                    }
                    if (status.getState() != null) {
                        status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
                        OrchestratorUtils.updateAndPublishExperimentStatus(processIdentity.getExperimentId(), status, statusPublisher, processIdentity.getGatewayId());
                        logger.info("expId : " + processIdentity.getExperimentId() + " :- Experiment status updated to " +
                                status.getState());
                    }
                } catch (TException e) {
                    logger.error("Message Id : " + message.getMessageId() + ", Message type : " + message.getType() +
                            "Error" + " while prcessing process status change event");
                    throw new RuntimeException("Error while updating experiment status", e);
                }
            } else {
                System.out.println("Message Recieved with message id " + message.getMessageId() + " and with message " +
                        "type " + message.getType().name());
            }
        }
    }

    private RegistryService.Client getRegistryServiceClient() {
        try {
            final int serverPort = Integer.parseInt(ServerSettings.getRegistryServerPort());
            final String serverHost = ServerSettings.getRegistryServerHost();
            return RegistryServiceClientFactory.createRegistryClient(serverHost, serverPort);
        } catch (RegistryServiceException | ApplicationSettingsException e) {
            throw new RuntimeException("Unable to create registry client...", e);
        }
    }

    public static ProcessModel cloneProcessFromExperiment(ExperimentModel experiment) {
        ProcessModel processModel = new ProcessModel();
        processModel.setCreationTime(experiment.getCreationTime());
        processModel.setExperimentId(experiment.getExperimentId());
        processModel.setApplicationInterfaceId(experiment.getExecutionId());
        processModel.setEnableEmailNotification(experiment.isEnableEmailNotification());
        List<String> emailAddresses = experiment.getEmailAddresses();
        if (emailAddresses != null && !emailAddresses.isEmpty()) {
            processModel.setEmailAddresses(emailAddresses);
        }
        List<InputDataObjectType> experimentInputs = experiment.getExperimentInputs();
        if (experimentInputs != null) {
            processModel.setProcessInputs(experimentInputs);
        }

        List<OutputDataObjectType> experimentOutputs = experiment.getExperimentOutputs();
        if (experimentOutputs != null) {
            processModel.setProcessOutputs(experimentOutputs);
        }

        UserConfigurationDataModel configData = experiment.getUserConfigurationData();
        if (configData != null) {
            processModel.setStorageResourceId(configData.getStorageId());
            processModel.setExperimentDataDir(configData.getExperimentDataDir());
            processModel.setGenerateCert(configData.isGenerateCert());
            processModel.setUserDn(configData.getUserDN());
            ComputationalResourceSchedulingModel scheduling = configData.getComputationalResourceScheduling();
            if (scheduling != null) {
                processModel.setProcessResourceSchedule(scheduling);
                processModel.setComputeResourceId(scheduling.getResourceHostId());
            }
            processModel.setUseUserCRPref(configData.isUseUserCRPref());
            processModel.setGroupResourceProfileId(configData.getGroupResourceProfileId());
        }
        processModel.setUserName(experiment.getUserName());
        return processModel;
    }

    public List<ProcessModel> createProcesses(String experimentId, String gatewayId) throws OrchestratorException {
        final RegistryService.Client registryClient = getRegistryServiceClient();
        try {//
            ExperimentModel experimentModel = registryClient.getExperiment(experimentId);
            List<ProcessModel> processModels = registryClient.getProcessList(experimentId);
            if (processModels == null || processModels.isEmpty()) {
                ProcessModel processModel = cloneProcessFromExperiment(experimentModel);
                String processId = registryClient.addProcess(processModel, experimentId);
                processModel.setProcessId(processId);
                processModels = new ArrayList<>();
                processModels.add(processModel);
            }
            return processModels;
        } catch (Exception e) {
            throw new OrchestratorException("Error during creating process", e);
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
    }

    public String createAndSaveTasks(String gatewayId, ProcessModel processModel, boolean autoSchedule) throws OrchestratorException {
        final RegistryService.Client registryClient = getRegistryServiceClient();
        try {
            ComputationalResourceSchedulingModel resourceSchedule = processModel.getProcessResourceSchedule();
            String userGivenQueueName = resourceSchedule.getQueueName();
            int userGivenWallTime = resourceSchedule.getWallTimeLimit();
            String resourceHostId = resourceSchedule.getResourceHostId();
            if (resourceHostId == null) {
                throw new OrchestratorException("Compute Resource Id cannot be null at this point");
            }
            ComputeResourceDescription computeResource = registryClient.getComputeResource(resourceHostId);
            JobSubmissionInterface preferredJobSubmissionInterface =
                    org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getPreferredJobSubmissionInterface(processModel, gatewayId);
            ComputeResourcePreference resourcePreference =
                    org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getComputeResourcePreference(processModel, gatewayId);
            List<String> taskIdList = new ArrayList<>();

            if (resourcePreference.getPreferredJobSubmissionProtocol() == JobSubmissionProtocol.UNICORE) {
                // TODO - breakdown unicore all in one task to multiple tasks, then we don't need to handle UNICORE here.
                taskIdList.addAll(createAndSaveSubmissionTasks(registryClient, gatewayId, preferredJobSubmissionInterface, processModel, userGivenWallTime));
            } else {
                taskIdList.addAll(createAndSaveEnvSetupTask(registryClient, gatewayId, processModel));
                taskIdList.addAll(createAndSaveInputDataStagingTasks(processModel, gatewayId));
                if (autoSchedule) {
                    List<BatchQueue> definedBatchQueues = computeResource.getBatchQueues();
                    for (BatchQueue batchQueue : definedBatchQueues) {
                        if (batchQueue.getQueueName().equals(userGivenQueueName)) {
                            int maxRunTime = batchQueue.getMaxRunTime();
                            if (maxRunTime < userGivenWallTime) {
                                resourceSchedule.setWallTimeLimit(maxRunTime);
                                // need to create more job submissions
                                int numOfMaxWallTimeJobs = ((int) Math.floor(userGivenWallTime / maxRunTime));
                                for (int i = 1; i <= numOfMaxWallTimeJobs; i++) {
                                    taskIdList.addAll(
                                            createAndSaveSubmissionTasks(registryClient, gatewayId, preferredJobSubmissionInterface, processModel, maxRunTime));
                                }
                                int leftWallTime = userGivenWallTime % maxRunTime;
                                if (leftWallTime != 0) {
                                    taskIdList.addAll(
                                            createAndSaveSubmissionTasks(registryClient, gatewayId, preferredJobSubmissionInterface, processModel, leftWallTime));
                                }
                            } else {
                                taskIdList.addAll(
                                        createAndSaveSubmissionTasks(registryClient, gatewayId, preferredJobSubmissionInterface, processModel, userGivenWallTime));
                            }
                        }
                    }
                } else {
                    taskIdList.addAll(createAndSaveSubmissionTasks(registryClient, gatewayId, preferredJobSubmissionInterface, processModel, userGivenWallTime));
                }
                taskIdList.addAll(createAndSaveOutputDataStagingTasks(processModel, gatewayId));
            }
            // update process scheduling
            registryClient.updateProcess(processModel, processModel.getProcessId());
            return getTaskDag(taskIdList);
        } catch (Exception e) {
            throw new OrchestratorException("Error during creating process", e);
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
    }

    private List<String> createAndSaveSubmissionTasks(RegistryService.Client registryClient, String gatewayId,
                                                      JobSubmissionInterface jobSubmissionInterface,
                                                      ProcessModel processModel,
                                                      int wallTime)
            throws TException, OrchestratorException {

        JobSubmissionProtocol jobSubmissionProtocol = jobSubmissionInterface.getJobSubmissionProtocol();
        MonitorMode monitorMode = null;
        if (jobSubmissionProtocol == JobSubmissionProtocol.SSH || jobSubmissionProtocol == JobSubmissionProtocol.SSH_FORK) {
            SSHJobSubmission sshJobSubmission = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getSSHJobSubmission(jobSubmissionInterface.getJobSubmissionInterfaceId());
            monitorMode = sshJobSubmission.getMonitorMode();
        } else if (jobSubmissionProtocol == JobSubmissionProtocol.UNICORE) {
            monitorMode = MonitorMode.FORK;
        } else if (jobSubmissionProtocol == JobSubmissionProtocol.LOCAL) {
            monitorMode = MonitorMode.LOCAL;
        } else if (jobSubmissionProtocol == JobSubmissionProtocol.CLOUD) {
            monitorMode = MonitorMode.CLOUD_JOB_MONITOR;
        } else {
            logger.error("expId : {}, processId : {} :- Unsupported Job submission protocol {}.",
                    processModel.getExperimentId(), processModel.getProcessId(), jobSubmissionProtocol.name());
            throw new OrchestratorException("Unsupported Job Submission Protocol " + jobSubmissionProtocol.name());
        }
        List<String> submissionTaskIds = new ArrayList<>();
        TaskModel taskModel = new TaskModel();
        taskModel.setParentProcessId(processModel.getProcessId());
        taskModel.setCreationTime(new Date().getTime());
        taskModel.setLastUpdateTime(taskModel.getCreationTime());
        TaskStatus taskStatus = new TaskStatus(TaskState.CREATED);
        taskStatus.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
        taskModel.setTaskStatuses(Arrays.asList(taskStatus));
        taskModel.setTaskType(TaskTypes.JOB_SUBMISSION);
        JobSubmissionTaskModel submissionSubTask = new JobSubmissionTaskModel();
        submissionSubTask.setMonitorMode(monitorMode);
        submissionSubTask.setJobSubmissionProtocol(jobSubmissionProtocol);
        submissionSubTask.setWallTime(wallTime);
        byte[] bytes = ThriftUtils.serializeThriftObject(submissionSubTask);
        taskModel.setSubTaskModel(bytes);
        String taskId = registryClient.addTask(taskModel, processModel.getProcessId());
        taskModel.setTaskId(taskId);
        submissionTaskIds.add(taskModel.getTaskId());

        // create monitor task for this Email based monitor mode job
        if (monitorMode == MonitorMode.JOB_EMAIL_NOTIFICATION_MONITOR
                || monitorMode == MonitorMode.CLOUD_JOB_MONITOR) {
            TaskModel monitorTaskModel = new TaskModel();
            monitorTaskModel.setParentProcessId(processModel.getProcessId());
            monitorTaskModel.setCreationTime(new Date().getTime());
            monitorTaskModel.setLastUpdateTime(monitorTaskModel.getCreationTime());
            TaskStatus monitorTaskStatus = new TaskStatus(TaskState.CREATED);
            monitorTaskStatus.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
            monitorTaskModel.setTaskStatuses(Arrays.asList(monitorTaskStatus));
            monitorTaskModel.setTaskType(TaskTypes.MONITORING);
            MonitorTaskModel monitorSubTaskModel = new MonitorTaskModel();
            monitorSubTaskModel.setMonitorMode(monitorMode);
            monitorTaskModel.setSubTaskModel(ThriftUtils.serializeThriftObject(monitorSubTaskModel));
            String mTaskId = (String) registryClient.addTask(monitorTaskModel, processModel.getProcessId());
            monitorTaskModel.setTaskId(mTaskId);
            submissionTaskIds.add(monitorTaskModel.getTaskId());
        }

        return submissionTaskIds;
    }

    private List<String> createAndSaveEnvSetupTask(RegistryService.Client registryClient, String gatewayId,
                                                   ProcessModel processModel)
            throws TException, AiravataException, OrchestratorException {
        List<String> envTaskIds = new ArrayList<>();
        TaskModel envSetupTask = new TaskModel();
        envSetupTask.setTaskType(TaskTypes.ENV_SETUP);
        envSetupTask.setTaskStatuses(Arrays.asList(new TaskStatus(TaskState.CREATED)));
        envSetupTask.setCreationTime(AiravataUtils.getCurrentTimestamp().getTime());
        envSetupTask.setLastUpdateTime(AiravataUtils.getCurrentTimestamp().getTime());
        envSetupTask.setParentProcessId(processModel.getProcessId());
        EnvironmentSetupTaskModel envSetupSubModel = new EnvironmentSetupTaskModel();
        envSetupSubModel.setProtocol(org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getSecurityProtocol(processModel, gatewayId));
        ComputeResourcePreference computeResourcePreference = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getComputeResourcePreference(processModel, gatewayId);
        String scratchLocation = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getScratchLocation(processModel, gatewayId);
        String workingDir = scratchLocation + File.separator + processModel.getProcessId();
        envSetupSubModel.setLocation(workingDir);
        byte[] envSetupSub = ThriftUtils.serializeThriftObject(envSetupSubModel);
        envSetupTask.setSubTaskModel(envSetupSub);
        String envSetupTaskId = (String) registryClient.addTask(envSetupTask, processModel.getProcessId());
        envSetupTask.setTaskId(envSetupTaskId);
        envTaskIds.add(envSetupTaskId);
        return envTaskIds;
    }

    public List<String> createAndSaveInputDataStagingTasks(ProcessModel processModel, String gatewayId)
            throws AiravataException, OrchestratorException {

        List<String> dataStagingTaskIds = new ArrayList<>();
        List<InputDataObjectType> processInputs = processModel.getProcessInputs();

        sortByInputOrder(processInputs);
        if (processInputs != null) {
            for (InputDataObjectType processInput : processInputs) {
                DataType type = processInput.getType();
                switch (type) {
                    case STDERR:
                        break;
                    case STDOUT:
                        break;
                    case URI:
                    case URI_COLLECTION:
                        final RegistryService.Client registryClient = getRegistryServiceClient();
                        try {
                            TaskModel inputDataStagingTask = getInputDataStagingTask(registryClient, processModel, processInput, gatewayId);
                            String taskId = registryClient
                                    .addTask(inputDataStagingTask, processModel.getProcessId());
                            inputDataStagingTask.setTaskId(taskId);
                            dataStagingTaskIds.add(inputDataStagingTask.getTaskId());
                        } catch (TException | TaskException e) {
                            throw new AiravataException("Error while serializing data staging sub task model", e);
                        } finally {
                            if (registryClient != null) {
                                ThriftUtils.close(registryClient);
                            }
                        }
                        break;
                    default:
                        // nothing to do
                        break;
                }
            }
        }
        return dataStagingTaskIds;
    }

    private void sortByInputOrder(List<InputDataObjectType> processInputs) {
        Collections.sort(processInputs, new Comparator<InputDataObjectType>() {
            @Override
            public int compare(InputDataObjectType inputDT_1, InputDataObjectType inputDT_2) {
                return inputDT_1.getInputOrder() - inputDT_2.getInputOrder();
            }
        });
    }

    private TaskModel getInputDataStagingTask(RegistryService.Client registryClient, ProcessModel processModel, InputDataObjectType processInput, String gatewayId) throws TException, TaskException, AiravataException, OrchestratorException {
        // create new task model for this task
        TaskModel taskModel = new TaskModel();
        taskModel.setParentProcessId(processModel.getProcessId());
        taskModel.setCreationTime(AiravataUtils.getCurrentTimestamp().getTime());
        taskModel.setLastUpdateTime(taskModel.getCreationTime());
        TaskStatus taskStatus = new TaskStatus(TaskState.CREATED);
        taskStatus.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
        taskModel.setTaskStatuses(Arrays.asList(taskStatus));
        taskModel.setTaskType(TaskTypes.DATA_STAGING);
        // create data staging sub task model
        DataStagingTaskModel submodel = new DataStagingTaskModel();
        ComputeResourceDescription computeResource = registryClient.
                getComputeResource(processModel.getComputeResourceId());
        String scratchLocation = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getScratchLocation(processModel, gatewayId);
        String workingDir = (scratchLocation.endsWith(File.separator) ? scratchLocation : scratchLocation + File.separator) +
                processModel.getProcessId() + File.separator;
        URI destination = null;
        try {
            DataMovementProtocol dataMovementProtocol =
                    org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getPreferredDataMovementProtocol(processModel, gatewayId);
            String loginUserName = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getLoginUserName(processModel, gatewayId);
            destination = new URI(dataMovementProtocol.name(),
                    loginUserName,
                    computeResource.getHostName(),
                    org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getDataMovementPort(processModel, gatewayId),
                    workingDir, null, null);
        } catch (URISyntaxException e) {
            throw new TaskException("Error while constructing destination file URI", e);
        }
        submodel.setType(DataStageType.INPUT);
        submodel.setSource(processInput.getValue());
        submodel.setProcessInput(processInput);
        submodel.setDestination(destination.toString());
        taskModel.setSubTaskModel(ThriftUtils.serializeThriftObject(submodel));
        return taskModel;
    }

    private String getTaskDag(List<String> taskIdList) {
        if (taskIdList.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String s : taskIdList) {
            sb.append(s).append(","); // comma separated values
        }
        String dag = sb.toString();
        return dag.substring(0, dag.length() - 1); // remove last comma
    }

    public List<String> createAndSaveOutputDataStagingTasks(ProcessModel processModel, String gatewayId)
            throws AiravataException, TException, OrchestratorException {

        final RegistryService.Client registryClient = getRegistryServiceClient();
        List<String> dataStagingTaskIds = new ArrayList<>();
        try {
            List<OutputDataObjectType> processOutputs = processModel.getProcessOutputs();
            String appName = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getApplicationInterfaceName(processModel);
            if (processOutputs != null) {
                for (OutputDataObjectType processOutput : processOutputs) {
                    DataType type = processOutput.getType();
                    switch (type) {
                        case STDOUT:
                            if (null == processOutput.getValue() || processOutput.getValue().trim().isEmpty()) {
                                processOutput.setValue(appName + ".stdout");
                            }
                            createOutputDataSatagingTasks(registryClient, processModel, gatewayId, dataStagingTaskIds, processOutput);
                            break;
                        case STDERR:
                            if (null == processOutput.getValue() || processOutput.getValue().trim().isEmpty()) {
                                processOutput.setValue(appName + ".stderr");
                            }
                            createOutputDataSatagingTasks(registryClient, processModel, gatewayId, dataStagingTaskIds, processOutput);
                            break;
                        case URI:
                            createOutputDataSatagingTasks(registryClient, processModel, gatewayId, dataStagingTaskIds, processOutput);
                            break;
                        default:
                            // nothing to do
                            break;
                    }
                }
            }

            try {
                if (isArchive(registryClient, processModel)) {
                    createArchiveDataStatgingTask(registryClient, processModel, gatewayId, dataStagingTaskIds);
                }
            } catch (Exception e) {
                throw new AiravataException("Error! Application interface retrieval failed", e);
            }
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
        return dataStagingTaskIds;
    }

    private void createOutputDataSatagingTasks(RegistryService.Client registryClient, ProcessModel processModel,
                                               String gatewayId,
                                               List<String> dataStagingTaskIds,
                                               OutputDataObjectType processOutput) throws AiravataException, OrchestratorException {
        try {
            TaskModel outputDataStagingTask = getOutputDataStagingTask(registryClient, processModel, processOutput, gatewayId);
            String taskId = registryClient
                    .addTask(outputDataStagingTask, processModel.getProcessId());
            outputDataStagingTask.setTaskId(taskId);
            dataStagingTaskIds.add(outputDataStagingTask.getTaskId());
        } catch (TException e) {
            throw new AiravataException("Error while serializing data staging sub task model", e);
        }
    }

    private TaskModel getOutputDataStagingTask(RegistryService.Client registryClient, ProcessModel processModel, OutputDataObjectType processOutput, String gatewayId) throws TException, AiravataException, OrchestratorException {
        try {

            // create new task model for this task
            TaskModel taskModel = new TaskModel();
            taskModel.setParentProcessId(processModel.getProcessId());
            taskModel.setCreationTime(AiravataUtils.getCurrentTimestamp().getTime());
            taskModel.setLastUpdateTime(taskModel.getCreationTime());
            TaskStatus taskStatus = new TaskStatus(TaskState.CREATED);
            taskStatus.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
            taskModel.setTaskStatuses(Arrays.asList(taskStatus));
            taskModel.setTaskType(TaskTypes.DATA_STAGING);
            ComputeResourceDescription computeResource = registryClient.
                    getComputeResource(processModel.getComputeResourceId());

            String workingDir = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getScratchLocation(processModel, gatewayId)
                    + File.separator + processModel.getProcessId() + File.separator;
            DataStagingTaskModel submodel = new DataStagingTaskModel();
            DataMovementProtocol dataMovementProtocol = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getPreferredDataMovementProtocol(processModel, gatewayId);
            URI source = null;
            try {
                String loginUserName = org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getLoginUserName(processModel, gatewayId);
                if (processOutput != null) {
                    submodel.setType(DataStageType.OUPUT);
                    submodel.setProcessOutput(processOutput);
                    source = new URI(dataMovementProtocol.name(),
                            loginUserName,
                            computeResource.getHostName(),
                            org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getDataMovementPort(processModel, gatewayId),
                            workingDir + processOutput.getValue(), null, null);
                } else {
                    // archive
                    submodel.setType(DataStageType.ARCHIVE_OUTPUT);
                    source = new URI(dataMovementProtocol.name(),
                            loginUserName,
                            computeResource.getHostName(),
                            org.apache.airavata.orchestrator.core.utils.OrchestratorUtils.getDataMovementPort(processModel, gatewayId),
                            workingDir, null, null);
                }
            } catch (URISyntaxException e) {
                throw new TaskException("Error while constructing source file URI", e);
            }
            // We don't know destination location at this time, data staging task will set this.
            // because destination is required field we set dummy destination
            submodel.setSource(source.toString());
            // We don't know destination location at this time, data staging task will set this.
            // because destination is required field we set dummy destination
            submodel.setDestination("dummy://temp/file/location");
            taskModel.setSubTaskModel(ThriftUtils.serializeThriftObject(submodel));
            return taskModel;
        } catch (TaskException e) {
            throw new OrchestratorException("Error occurred while retrieving data movement from app catalog", e);
        }
    }

    private void createArchiveDataStatgingTask(RegistryService.Client registryClient, ProcessModel processModel,
                                               String gatewayId,
                                               List<String> dataStagingTaskIds) throws AiravataException, TException, OrchestratorException {
        TaskModel archiveTask = null;
        try {
            archiveTask = getOutputDataStagingTask(registryClient, processModel, null, gatewayId);
        } catch (TException e) {
            throw new AiravataException("Error! DataStaging sub task serialization failed", e);
        }
        String taskId = registryClient
                .addTask(archiveTask, processModel.getProcessId());
        archiveTask.setTaskId(taskId);
        dataStagingTaskIds.add(archiveTask.getTaskId());

    }

    private boolean isArchive(RegistryService.Client registryClient, ProcessModel processModel) throws TException {
        ApplicationInterfaceDescription appInterface = registryClient
                .getApplicationInterface(processModel.getApplicationInterfaceId());
        return appInterface.isArchiveWorkingDirectory();
    }
}
