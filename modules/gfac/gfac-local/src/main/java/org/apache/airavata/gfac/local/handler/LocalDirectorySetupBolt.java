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
package org.apache.airavata.gfac.local.handler;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.airavata.appcatalog.cpi.AppCatalog;
import org.airavata.appcatalog.cpi.AppCatalogException;
import org.apache.aiaravata.application.catalog.data.impl.AppCatalogFactory;
import org.apache.airavata.gfac.Constants;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.local.utils.ExperimentModelUtil;
import org.apache.airavata.model.appcatalog.appdeployment.ApplicationDeploymentDescription;
import org.apache.airavata.model.appcatalog.appinterface.ApplicationInterfaceDescription;
import org.apache.airavata.model.appcatalog.computeresource.ComputeResourceDescription;
import org.apache.airavata.model.appcatalog.gatewayprofile.ComputeResourcePreference;
import org.apache.airavata.model.workspace.experiment.*;
import org.apache.airavata.persistance.registry.jpa.impl.RegistryFactory;
import org.apache.airavata.registry.cpi.ChildDataType;
import org.apache.airavata.registry.cpi.Registry;
import org.apache.airavata.registry.cpi.RegistryException;
import org.apache.airavata.registry.cpi.RegistryModelType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class LocalDirectorySetupBolt extends BaseBasicBolt {
    private final static Logger logger = LoggerFactory.getLogger(LocalDirectorySetupBolt.class);

    Registry registry;

    public LocalDirectorySetupBolt() throws RegistryException {
        registry = RegistryFactory.getDefaultRegistry();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            Registry registry = RegistryFactory.getDefaultRegistry();
            String Ids = tuple.getString(0);
            String[] split = Ids.split(",");
            if (split.length != 3) {
                throw new FailedException("Wrong tuple given: " + Ids);
            }
            String gatewayId = split[0];
            String expId = split[1];
            List<TaskDetails> tasks = createTasks(expId);
            String taskId = tasks.get(0).getTaskID();
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

            String scratchLocation = gatewayResourcePreferences.getScratchLocation();
            String workingDir = scratchLocation + File.separator + expId;
            String inputDir = workingDir + File.separator + Constants.INPUT_DATA_DIR_VAR_NAME;
            String outputDir = workingDir + File.separator + Constants.OUTPUT_DATA_DIR_VAR_NAME;
            makeFileSystemDir(workingDir);
            makeFileSystemDir(inputDir);
            makeFileSystemDir(outputDir);

            basicOutputCollector.emit(new Values(Ids, taskId));

        } catch (RegistryException e) {
            logger.error(e.getMessage(), e);
            throw new FailedException(e);
        } catch (AppCatalogException e) {
            logger.error(e.getMessage(), e);
            throw new FailedException(e);
        } catch (GFacHandlerException e) {
            logger.error(e.getMessage(), e);
            throw new FailedException(e);
        }
    }

    public List<TaskDetails> createTasks(String experimentId) throws RegistryException {
        Experiment experiment = null;
        List<TaskDetails> tasks = new ArrayList<TaskDetails>();
        experiment = (Experiment) registry.get(RegistryModelType.EXPERIMENT, experimentId);


        WorkflowNodeDetails iDontNeedaNode = ExperimentModelUtil.createWorkflowNode("IDontNeedaNode", null);
        String nodeID = (String) registry.add(ChildDataType.WORKFLOW_NODE_DETAIL, iDontNeedaNode, experimentId);

        TaskDetails taskDetails = ExperimentModelUtil.cloneTaskFromExperiment(experiment);
        taskDetails.setTaskID((String) registry.add(ChildDataType.TASK_DETAIL, taskDetails, nodeID));
        tasks.add(taskDetails);
        return tasks;
    }


    private void makeFileSystemDir(String dir) throws GFacHandlerException {
        File f = new File(dir);
        if (f.isDirectory() && f.exists()) {
            return;
        } else if (!new File(dir).mkdir()) {
            throw new GFacHandlerException("Cannot create directory " + dir);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("experimentId","taskId"));
    }
}
