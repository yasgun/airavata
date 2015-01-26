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
 *
*/
package org.apache.airavata.gfac.local.provider.impl;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.airavata.appcatalog.cpi.AppCatalog;
import org.apache.aiaravata.application.catalog.data.impl.AppCatalogFactory;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.local.utils.InputStreamToFileWriter;
import org.apache.airavata.gfac.local.utils.InputUtils;
import org.apache.airavata.model.appcatalog.appdeployment.ApplicationDeploymentDescription;
import org.apache.airavata.model.appcatalog.appdeployment.SetEnvPaths;
import org.apache.airavata.model.appcatalog.appinterface.ApplicationInterfaceDescription;
import org.apache.airavata.model.appcatalog.appinterface.InputDataObjectType;
import org.apache.airavata.model.appcatalog.computeresource.ComputeResourceDescription;
import org.apache.airavata.model.appcatalog.gatewayprofile.ComputeResourcePreference;
import org.apache.airavata.model.workspace.experiment.*;
import org.apache.airavata.persistance.registry.jpa.impl.RegistryFactory;
import org.apache.airavata.registry.cpi.Registry;
import org.apache.airavata.registry.cpi.RegistryException;
import org.apache.airavata.registry.cpi.RegistryModelType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class LocalProviderBolt extends BaseBasicBolt {
    private final static Logger logger = LoggerFactory.getLogger(LocalProviderBolt.class);

    private ProcessBuilder builder;
    private List<String> cmdList;
    private String jobId;
    private Registry registry;

    public static class LocalProviderJobData {
        private String applicationName;
        private List<String> inputParameters;
        private String workingDir;
        private String inputDir;
        private String outputDir;

        public String getApplicationName() {
            return applicationName;
        }

        public void setApplicationName(String applicationName) {
            this.applicationName = applicationName;
        }

        public List<String> getInputParameters() {
            return inputParameters;
        }

        public void setInputParameters(List<String> inputParameters) {
            this.inputParameters = inputParameters;
        }

        public String getWorkingDir() {
            return workingDir;
        }

        public void setWorkingDir(String workingDir) {
            this.workingDir = workingDir;
        }

        public String getInputDir() {
            return inputDir;
        }

        public void setInputDir(String inputDir) {
            this.inputDir = inputDir;
        }

        public String getOutputDir() {
            return outputDir;
        }

        public void setOutputDir(String outputDir) {
            this.outputDir = outputDir;
        }
    }

    public LocalProviderBolt() throws RegistryException {
        cmdList = new ArrayList<String>();
        registry = RegistryFactory.getDefaultRegistry();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            registry = RegistryFactory.getDefaultRegistry();
            String ids = tuple.getString(0);
            String taskId = tuple.getString(1);

            String[] split = ids.split(",");
            if (split.length != 3) {
                throw new FailedException("Wrong tuple given: " + ids);
            }
            String gatewayId = split[0];
            String experimentId = split[1];

            TaskDetails taskData = (TaskDetails) registry.get(RegistryModelType.TASK_DETAIL, taskId);

            String applicationInterfaceId = taskData.getApplicationId();
            String applicationDeploymentId = taskData.getApplicationDeploymentId();
            if (null == applicationInterfaceId) {
                throw new FailedException("Error executing the job. The required Application Id is missing");
            }
            if (null == applicationDeploymentId) {
                throw new FailedException("Error executing the job. The required Application deployment Id is missing");
            }

            AppCatalog appCatalog = AppCatalogFactory.getAppCatalog();
            //fetch the compute resource, application interface and deployment information from app catalog
            ApplicationInterfaceDescription applicationInterface = appCatalog.
                    getApplicationInterface().getApplicationInterface(applicationInterfaceId);
            ApplicationDeploymentDescription applicationDeployment = appCatalog.
                    getApplicationDeployment().getApplicationDeployement(applicationDeploymentId);
            ComputeResourceDescription computeResource = appCatalog.getComputeResource().
                    getComputeResource(applicationDeployment.getComputeHostId());
            ComputeResourcePreference gatewayResourcePreferences = appCatalog.getGatewayProfile().
                    getComputeResourcePreference(gatewayId, applicationDeployment.getComputeHostId());

            if (gatewayResourcePreferences == null) {
                List<String> gatewayProfileIds = appCatalog.getGatewayProfile()
                        .getGatewayProfileIds(gatewayId);
                for (String profileId : gatewayProfileIds) {
                    gatewayId = profileId;
                    gatewayResourcePreferences = appCatalog.getGatewayProfile().
                            getComputeResourcePreference(gatewayId, applicationDeployment.getComputeHostId());
                    if (gatewayResourcePreferences != null) {
                        break;
                    }
                }
            }
            String executablePath = applicationDeployment.getExecutablePath();


            buildCommand(applicationDeployment, taskId);
            initProcessBuilder(applicationDeployment);

            // extra environment variables
            String workingDir = "/tmp";
            workingDir = workingDir + experimentId + File.separator + org.apache.airavata.gfac.Constants.INPUT_DATA_DIR_VAR_NAME;
            String stdOutFile = workingDir + File.separator + applicationInterface.getApplicationName().replaceAll("\\s+", "") + ".stdout";
            String stdErrFile = workingDir + File.separator + applicationInterface.getApplicationName().replaceAll("\\s+", "") + ".stderr";

            builder.environment().put(org.apache.airavata.gfac.Constants.INPUT_DATA_DIR_VAR_NAME, stdOutFile);
            builder.environment().put(org.apache.airavata.gfac.Constants.OUTPUT_DATA_DIR_VAR_NAME, stdErrFile);

            // set working directory
            builder.directory(new File(workingDir));

            // log info
            logger.info("Command = " + InputUtils.buildCommand(cmdList));
            logger.info("Working dir = " + builder.directory());
            for (String key : builder.environment().keySet()) {
                logger.info("Env[" + key + "] = " + builder.environment().get(key));
            }


            JobDetails jobDetails = new JobDetails();
            jobId = taskData.getTaskID();
            jobDetails.setJobID(jobId);
            jobDetails.setJobDescription(applicationDeployment.getAppDeploymentDescription());
            GFacUtils.saveJobStatus(registry, jobDetails, taskId, JobState.SETUP);
            // running cmd
            Process process = builder.start();

            Thread standardOutWriter = new InputStreamToFileWriter(process.getInputStream(), stdOutFile);
            Thread standardErrorWriter = new InputStreamToFileWriter(process.getErrorStream(), stdErrFile);

            // start output threads
            standardOutWriter.setDaemon(true);
            standardErrorWriter.setDaemon(true);
            standardOutWriter.start();
            standardErrorWriter.start();

            int returnValue = process.waitFor();

            // make sure other two threads are done
            standardOutWriter.join();
            standardErrorWriter.join();

            /*
             * check return value. usually not very helpful to draw conclusions based on return values so don't bother.
             * just provide warning in the log messages
             */
            if (returnValue != 0) {
                logger.error("Process finished with non zero return value. Process may have failed");
            } else {
                logger.info("Process finished with return value of zero.");
            }

            StringBuffer buf = new StringBuffer();
            buf.append("Executed ").append(InputUtils.buildCommand(cmdList))
                    .append(" on the localHost, working directory = ").append(workingDir)
                    .append(" tempDirectory = ").append(workingDir).append(" With the status ")
                    .append(String.valueOf(returnValue));

            logger.info(buf.toString());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


    private void buildCommand(ApplicationDeploymentDescription applicationDeploymentDescription, String taskID) throws RegistryException, GFacException {
        cmdList.add(applicationDeploymentDescription.getExecutablePath());
        String appModuleId = applicationDeploymentDescription.getAppModuleId();
        TaskDetails taskData = (TaskDetails) registry.get(RegistryModelType.TASK_DETAIL, taskID);

        List<InputDataObjectType> taskInputs = taskData.getApplicationInputs();
        Map<String, Object> inputParamMap = GFacUtils.getInputParamMap(taskInputs);

        // sort the inputs first and then build the command List
        Comparator<InputDataObjectType> inputOrderComparator = new Comparator<InputDataObjectType>() {
            @Override
            public int compare(InputDataObjectType inputDataObjectType, InputDataObjectType t1) {
                return inputDataObjectType.getInputOrder() - t1.getInputOrder();
            }
        };
        Set<InputDataObjectType> sortedInputSet = new TreeSet<InputDataObjectType>(inputOrderComparator);
        for (Object object : inputParamMap.values()) {
            if (object instanceof InputDataObjectType) {
                InputDataObjectType inputDOT = (InputDataObjectType) object;
                sortedInputSet.add(inputDOT);
            }
        }
        for (InputDataObjectType inputDataObjectType : sortedInputSet) {
            if (inputDataObjectType.getApplicationArgument() != null
                    && !inputDataObjectType.getApplicationArgument().equals("")) {
                cmdList.add(inputDataObjectType.getApplicationArgument());
            }

            if (inputDataObjectType.getValue() != null
                    && !inputDataObjectType.getValue().equals("")) {
                cmdList.add(inputDataObjectType.getValue());
            }
        }

    }

    private void initProcessBuilder(ApplicationDeploymentDescription app) {
        builder = new ProcessBuilder(cmdList);

        List<SetEnvPaths> setEnvironment = app.getSetEnvironment();
        if (setEnvironment != null) {
            for (SetEnvPaths envPath : setEnvironment) {
                Map<String, String> builderEnv = builder.environment();
                builderEnv.put(envPath.getName(), envPath.getValue());
            }
        }
    }

}
