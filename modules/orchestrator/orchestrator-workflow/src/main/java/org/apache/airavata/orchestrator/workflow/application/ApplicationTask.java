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
import org.apache.airavata.common.utils.*;
import org.apache.airavata.gfac.core.GFacUtils;
import org.apache.airavata.gfac.core.scheduler.HostScheduler;
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
import org.apache.airavata.model.data.replica.DataProductModel;
import org.apache.airavata.model.data.replica.DataReplicaLocationModel;
import org.apache.airavata.model.data.replica.ReplicaLocationCategory;
import org.apache.airavata.model.experiment.ExperimentModel;
import org.apache.airavata.model.experiment.UserConfigurationDataModel;
import org.apache.airavata.model.process.ProcessModel;
import org.apache.airavata.model.scheduling.ComputationalResourceSchedulingModel;
import org.apache.airavata.model.status.ExperimentState;
import org.apache.airavata.model.status.ExperimentStatus;
import org.apache.airavata.model.workflow.WorkflowApplication;
import org.apache.airavata.orchestrator.core.exception.OrchestratorException;
import org.apache.airavata.orchestrator.workflow.core.JobSubmitter;
import org.apache.airavata.orchestrator.workflow.core.WorkflowTask;
import org.apache.airavata.orchestrator.workflow.util.WorkflowUtils;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.helix.task.TaskResult;
import org.apache.thrift.TException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@TaskDef(name = "Application Processor Task")
public class ApplicationTask extends WorkflowTask {

    private static Logger logger = LoggerFactory.getLogger(ApplicationTask.class);

    private Publisher statusPublisher;
    private CuratorFramework curatorClient;
    private JobSubmitter jobSubmitter;

    RegistryService.Client registryClient;

    private WorkflowApplication workflowApplication;

    @Override
    public TaskResult onRun(TaskHelper helper) {
        try {
            initialize();
            for (WorkflowApplication application : registryClient.getWorkflow(getWorkflowId()).getApplications()) {
                if (application.getId().equals(getTaskId())) {
                    workflowApplication = application;
                    break;
                }
            }

            //application logic goes here
            launchApplication();

            //need to wait for process status - COMPLETED

            WorkflowUtils.getRegistryClientPool().returnResource(registryClient);
        } catch (Exception e) {
            try {
                WorkflowUtils.getRegistryClientPool().returnBrokenResource(registryClient);
            } catch (ApplicationSettingsException e1) {
                logger.warn("Returning broken resource to the registry client pool failed", e);
            }
            return onFail("Application with id: " + getTaskId() + " on workflow with id: " + getWorkflowId() +
                    " and id: " + getWorkflowId() + " failed", false);
        }

        return onSuccess("Application with id: " + getTaskId() + " on workflow with id: " + getWorkflowId() + " and id: " +
                getWorkflowId() + " completed");
    }

    @Override
    public void onCancel() {
        logger.info(getExperimentId(), "Experiment: {} is cancelling  !!!!!", getExperimentId());
        try {
            validateStatesAndCancel(registryClient, getExperimentId(), getGatewayId());
        } catch (Exception e) {
            logger.error("expId : " + getExperimentId() + " :- Error while cancelling experiment", e);
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

    private void initialize() throws AiravataException, OrchestratorException {
        statusPublisher = MessagingFactory.getPublisher(Type.STATUS);
        startCurator();
        jobSubmitter = new ApplicationJobSubmitter();
        jobSubmitter.initialize();
        registryClient = WorkflowUtils.getRegistryClientPool().getResource();
    }

    private void startCurator() throws ApplicationSettingsException {
        String connectionSting = ServerSettings.getZookeeperConnection();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        curatorClient = CuratorFrameworkFactory.newClient(connectionSting, retryPolicy);
        curatorClient.start();
    }

    private void launchApplication() throws OrchestratorException {

        try {
            String experimentNodePath = GFacUtils.getExperimentNodePath(getExperimentId());
            ZKPaths.mkdirs(curatorClient.getZookeeperClient().getZooKeeper(), experimentNodePath);
            String experimentCancelNode = ZKPaths.makePath(experimentNodePath, ZkConstants.ZOOKEEPER_CANCEL_LISTENER_NODE);
            ZKPaths.mkdirs(curatorClient.getZookeeperClient().getZooKeeper(), experimentCancelNode);

            ComputeResourcePreference computeResourcePreference = registryClient.getGatewayComputeResourcePreference
                    (getGatewayId(), workflowApplication.getComputeResourceId());
            String token = computeResourcePreference.getResourceSpecificCredentialStoreToken();

            if (token == null || token.isEmpty()) {
                GatewayResourceProfile gatewayProfile = registryClient.getGatewayResourceProfile(getGatewayId());
                token = gatewayProfile.getCredentialStoreToken();
            }

            if (token == null || token.isEmpty()) {
                logger.error("You have not configured credential store token at gateway profile or compute resource preference." +
                        " Please provide the correct token at gateway profile or compute resource preference.");
                throw new OrchestratorException("You have not configured credential store token at gateway profile or compute resource preference." +
                        " Please provide the correct token at gateway profile or compute resource preference.");
            }

            ProcessModel processModel = createProcess();

            processModel.getProcessInputs().forEach(pi -> {
                if (pi.getType().equals(DataType.URI) && pi.getValue().startsWith("airavata-dp://")) {
                    try {
                        DataProductModel dataProductModel = registryClient.getDataProduct(pi.getValue());
                        Optional<DataReplicaLocationModel> rpLocation = dataProductModel.getReplicaLocations().stream()
                                .filter(rpModel -> rpModel.getReplicaLocationCategory()
                                        .equals(ReplicaLocationCategory.GATEWAY_DATA_STORE)).findFirst();

                        if (rpLocation.isPresent()) {
                            pi.setValue(rpLocation.get().getFilePath());
                            pi.setStorageResourceId(rpLocation.get().getStorageResourceId());
                        } else {
                            logger.error("Could not find a replica for the URI " + pi.getValue());
                        }
                    } catch (TException e) {
                        throw new RuntimeException("Error while launching the application with id: " + getTaskId() +
                                " in experiment with id: " + getExperimentId(), e);
                    }

                } else if (pi.getType().equals(DataType.URI_COLLECTION) && pi.getValue().contains("airavata-dp://")) {
                    try {
                        String[] uriList = pi.getValue().split(",");
                        final ArrayList<String> filePathList = new ArrayList<>();

                        for (String uri : uriList) {
                            if (uri.startsWith("airavata-dp://")) {
                                DataProductModel dataProductModel = registryClient.getDataProduct(uri);
                                Optional<DataReplicaLocationModel> rpLocation = dataProductModel.getReplicaLocations()
                                        .stream().filter(rpModel -> rpModel.getReplicaLocationCategory()
                                                .equals(ReplicaLocationCategory.GATEWAY_DATA_STORE)).findFirst();

                                if (rpLocation.isPresent()) {
                                    filePathList.add(rpLocation.get().getFilePath());
                                } else {
                                    logger.error("Could not find a replica for the URI " + pi.getValue());
                                }
                            } else {
                                filePathList.add(uri);
                            }
                        }
                        pi.setValue(StringUtils.join(filePathList, ','));
                    } catch (TException e) {
                        throw new RuntimeException("Error while launching experiment", e);
                    }
                }
            });

            TaskDAGHandler taskDAGHandler = new TaskDAGHandler(registryClient);
            String taskDag = taskDAGHandler.createAndSaveTasks(getGatewayId(), processModel, false);
            processModel.setTaskDag(taskDag);
            registryClient.updateProcess(processModel, processModel.getProcessId());

            logger.debug("Launching application with id: " + getTaskId() + " for experiment with id: " + getExperimentId());

            ExperimentStatus status = new ExperimentStatus(ExperimentState.LAUNCHED);
            status.setReason("submitted all processes");
            status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
            WorkflowUtils.updateAndPublishExperimentStatus(getExperimentId(), status, statusPublisher, getGatewayId());
            logger.info("Launched application with id: " + getTaskId() + " for the experiment with id: " + getExperimentId());
            String applicationId = processModel.getApplicationInterfaceId();
            if (applicationId == null) {
                logger.error(workflowApplication.getProcessId(), "Application interface id shouldn't be null.");
                throw new OrchestratorException("Error executing the job, application interface id shouldn't be null.");
            }

            // set application deployment id to process model
            ApplicationDeploymentDescription applicationDeploymentDescription = getAppDeployment(registryClient, processModel, applicationId);
            processModel.setApplicationDeploymentId(applicationDeploymentDescription.getAppDeploymentId());

            // set compute resource id to process model, default we set the same in the user preferred compute host id
            processModel.setComputeResourceId(processModel.getProcessResourceSchedule().getResourceHostId());
            registryClient.updateProcess(processModel, processModel.getProcessId());

            jobSubmitter.submit(processModel.getExperimentId(), processModel.getProcessId(), token);

        } catch (Exception e) {
            ExperimentStatus status = new ExperimentStatus(ExperimentState.FAILED);
            status.setReason("Application launch failed");
            status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());

            try {
                WorkflowUtils.updateAndPublishExperimentStatus(getExperimentId(), status, statusPublisher, getGatewayId());
            } catch (TException e1) {
                logger.error("Updating and publishing experiment status failed for the experiment with id: " + getExperimentId(), e1);
            }

            logger.error("Application '" + getTaskId() + "' in experiment '" + getExperimentId() +
                    "' launch failed. Experiment failed to validate: ", e);
            throw new OrchestratorException("Application '" + getTaskId() + "' in experiment '" + getExperimentId() +
                    "' launch failed. Experiment failed to validate: ", e);
        }
    }

    private ApplicationDeploymentDescription getAppDeployment(RegistryService.Client registryClient, ProcessModel processModel, String applicationId)
            throws OrchestratorException, ClassNotFoundException, ApplicationSettingsException, InstantiationException, IllegalAccessException, TException {

        ApplicationInterfaceDescription applicationInterface = registryClient.getApplicationInterface(applicationId);
        List<String> applicationModules = applicationInterface.getApplicationModules();

        if (applicationModules.size() == 0) {
            throw new OrchestratorException(
                    "No modules defined for application "
                            + applicationId);
        }

        String selectedModuleId = applicationModules.get(0);
        List<ApplicationDeploymentDescription> applicationDeployments = registryClient.getApplicationDeployments(selectedModuleId);
        Map<ComputeResourceDescription, ApplicationDeploymentDescription> deploymentMap = new HashMap<ComputeResourceDescription, ApplicationDeploymentDescription>();

        for (ApplicationDeploymentDescription deploymentDescription : applicationDeployments) {
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
                // TODO deprecate this approach as we are replacing GFAC
                String expCancelNodePath = ZKPaths.makePath(ZKPaths.makePath(ZkConstants.ZOOKEEPER_EXPERIMENT_NODE,
                        experimentId), ZkConstants.ZOOKEEPER_CANCEL_LISTENER_NODE);
                Stat stat = curatorClient.checkExists().forPath(expCancelNodePath);
                if (stat != null) {
                    curatorClient.setData().withVersion(-1).forPath(expCancelNodePath, ZkConstants.ZOOKEEPER_CANCEL_REQEUST
                            .getBytes());
                    ExperimentStatus status = new ExperimentStatus(ExperimentState.CANCELING);
                    status.setReason("Experiment cancel request processed");
                    status.setTimeOfStateChange(AiravataUtils.getCurrentTimestamp().getTime());
                    WorkflowUtils.updateAndPublishExperimentStatus(experimentId, status, statusPublisher, gatewayId);
                    logger.info("expId : " + experimentId + " :- Experiment status updated to " + status.getState());
                    return true;
                }
                return false;
        }
    }

    public void cancelExperiment(ExperimentModel experiment, String tokenId) throws OrchestratorException {
        logger.info("Terminating experiment " + experiment.getExperimentId());
        try {
            List<String> processIds = registryClient.getProcessIds(experiment.getExperimentId());
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

    public ProcessModel createProcess() throws OrchestratorException {
        try {
            ExperimentModel experimentModel = registryClient.getExperiment(getExperimentId());
            ProcessModel processModel = registryClient.getProcess(workflowApplication.getProcessId());

            if (processModel == null) {
                processModel = new ProcessModel();
                processModel.setCreationTime(experimentModel.getCreationTime());
                processModel.setExperimentId(experimentModel.getExperimentId());
                processModel.setApplicationInterfaceId(experimentModel.getExecutionId());
                processModel.setEnableEmailNotification(experimentModel.isEnableEmailNotification());
                List<String> emailAddresses = experimentModel.getEmailAddresses();

                if (emailAddresses != null && !emailAddresses.isEmpty()) {
                    processModel.setEmailAddresses(emailAddresses);
                }
                List<InputDataObjectType> experimentInputs = experimentModel.getExperimentInputs();
                if (experimentInputs != null) {
                    processModel.setProcessInputs(experimentInputs);
                }

                List<OutputDataObjectType> experimentOutputs = experimentModel.getExperimentOutputs();
                if (experimentOutputs != null) {
                    processModel.setProcessOutputs(experimentOutputs);
                }

                UserConfigurationDataModel configData = experimentModel.getUserConfigurationData();
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
                processModel.setUserName(experimentModel.getUserName());
                String processId = registryClient.addProcess(processModel, getExperimentId());
                processModel.setProcessId(processId);
            }
            return processModel;
        } catch (Exception e) {
            throw new OrchestratorException("Error during creating process", e);
        } finally {
            if (registryClient != null) {
                ThriftUtils.close(registryClient);
            }
        }
    }
}
