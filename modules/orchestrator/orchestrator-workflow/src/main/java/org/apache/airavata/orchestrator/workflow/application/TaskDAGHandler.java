package org.apache.airavata.orchestrator.workflow.application;

import org.apache.airavata.common.exception.AiravataException;
import org.apache.airavata.common.utils.AiravataUtils;
import org.apache.airavata.common.utils.ThriftUtils;
import org.apache.airavata.gfac.core.task.TaskException;
import org.apache.airavata.model.appcatalog.appinterface.ApplicationInterfaceDescription;
import org.apache.airavata.model.appcatalog.computeresource.*;
import org.apache.airavata.model.application.io.DataType;
import org.apache.airavata.model.application.io.InputDataObjectType;
import org.apache.airavata.model.application.io.OutputDataObjectType;
import org.apache.airavata.model.data.movement.DataMovementProtocol;
import org.apache.airavata.model.process.ProcessModel;
import org.apache.airavata.model.scheduling.ComputationalResourceSchedulingModel;
import org.apache.airavata.model.status.TaskState;
import org.apache.airavata.model.status.TaskStatus;
import org.apache.airavata.model.task.*;
import org.apache.airavata.orchestrator.core.exception.OrchestratorException;
import org.apache.airavata.orchestrator.core.utils.OrchestratorUtils;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class TaskDAGHandler {

    private static Logger logger = LoggerFactory.getLogger(TaskDAGHandler.class);

    private RegistryService.Client registryClient;

    public TaskDAGHandler(RegistryService.Client registryClient) {
        this.registryClient = registryClient;
    }

    public String createAndSaveTasks(String gatewayId, ProcessModel processModel, boolean autoSchedule) throws OrchestratorException {
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
                    OrchestratorUtils.getPreferredJobSubmissionInterface(processModel, gatewayId);
            JobSubmissionProtocol preferredJobSubmissionProtocol = OrchestratorUtils.getPreferredJobSubmissionProtocol(processModel, gatewayId);
            List<String> taskIdList = new ArrayList<>();

            if (preferredJobSubmissionProtocol == JobSubmissionProtocol.UNICORE) {
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
        envSetupSubModel.setProtocol(OrchestratorUtils.getSecurityProtocol(processModel, gatewayId));
        String scratchLocation = OrchestratorUtils.getScratchLocation(processModel, gatewayId);
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
