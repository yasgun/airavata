package org.apache.airavata.registry.core.repositories.expcatalog;

import org.apache.airavata.model.experiment.ExperimentModel;
import org.apache.airavata.model.experiment.ExperimentStatistics;
import org.apache.airavata.model.experiment.ExperimentSummaryModel;
import org.apache.airavata.model.experiment.ExperimentType;
import org.apache.airavata.model.status.ExperimentState;
import org.apache.airavata.model.status.ExperimentStatus;
import org.apache.airavata.model.workspace.Gateway;
import org.apache.airavata.model.workspace.Project;
import org.apache.airavata.registry.core.repositories.expcatalog.util.Initialize;
import org.apache.airavata.registry.core.utils.DBConstants;
import org.apache.airavata.registry.cpi.RegistryException;
import org.apache.airavata.registry.cpi.ResultOrderType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ExperimentSummaryRepositoryTest {

    private static Initialize initialize;
    GatewayRepository gatewayRepository;
    ProjectRepository projectRepository;
    ExperimentRepository experimentRepository;
    ExperimentSummaryRepository experimentSummaryRepository;
    private static final Logger logger = LoggerFactory.getLogger(ExperimentSummaryRepositoryTest.class);

    @Before
    public void setUp() {
        try {
            initialize = new Initialize("expcatalog-derby.sql");
            initialize.initializeDB();
            gatewayRepository = new GatewayRepository();
            projectRepository = new ProjectRepository();
            experimentRepository = new ExperimentRepository();
            experimentSummaryRepository = new ExperimentSummaryRepository();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("********** TEAR DOWN ************");
        initialize.stopDerbyServer();
    }

    @Test
    public void ExperimentSummaryRepositoryTest() throws RegistryException {
        Gateway gateway = new Gateway();
        gateway.setGatewayId("gateway");
        gateway.setDomain("SEAGRID");
        gateway.setEmailAddress("abc@d.com");
        String gatewayId = gatewayRepository.addGateway(gateway);

        Project project = new Project();
        project.setName("projectName");
        project.setOwner("user");
        project.setGatewayId(gatewayId);
        String projectId = projectRepository.addProject(project, gatewayId);

        ExperimentModel experimentModelOne = new ExperimentModel();
        experimentModelOne.setProjectId(projectId);
        experimentModelOne.setGatewayId(gatewayId);
        experimentModelOne.setExperimentType(ExperimentType.SINGLE_APPLICATION);
        experimentModelOne.setUserName("userOne");
        experimentModelOne.setExperimentName("nameOne");
        experimentModelOne.setDescription("descriptionOne");
        experimentModelOne.setExecutionId("executionIdOne");

        ExperimentModel experimentModelTwo = new ExperimentModel();
        experimentModelTwo.setProjectId(projectId);
        experimentModelTwo.setGatewayId(gatewayId);
        experimentModelTwo.setExperimentType(ExperimentType.WORKFLOW);
        experimentModelTwo.setUserName("userTwo");
        experimentModelTwo.setExperimentName("nameTwo");
        experimentModelTwo.setDescription("descriptionTwo");
        experimentModelTwo.setExecutionId("executionIdTwo");

        String experimentIdOne = experimentRepository.addExperiment(experimentModelOne);
        assertTrue(experimentIdOne != null);

        String expertimentIdTwo = experimentRepository.addExperiment(experimentModelTwo);
        assertTrue(expertimentIdTwo != null);

        Timestamp timeOne = Timestamp.valueOf("2010-01-01 09:00:00");
        experimentModelOne.setCreationTime(timeOne.getTime());
        experimentRepository.updateExperiment(experimentModelOne, experimentIdOne);

        Timestamp timeTwo = Timestamp.valueOf("2018-01-01 09:00:00");
        experimentModelTwo.setCreationTime(timeTwo.getTime());
        experimentRepository.updateExperiment(experimentModelTwo, expertimentIdTwo);

        Map<String, String> filters = new HashMap<>();
        filters.put(DBConstants.Experiment.GATEWAY_ID, gatewayId);
        filters.put(DBConstants.Experiment.PROJECT_ID, projectId);

        List<ExperimentSummaryModel> experimentSummaryModelList = experimentSummaryRepository.
                searchExperiments(filters, -1, 0, null, null);
        assertTrue(experimentSummaryModelList.size() == 2);

        filters.put(DBConstants.Experiment.EXECUTION_ID, "executionIdTwo");

        experimentSummaryModelList = experimentSummaryRepository.
                searchExperiments(filters, -1, 0, null, null);
        assertTrue(experimentSummaryModelList.size() == 1);
        assertEquals(expertimentIdTwo, experimentSummaryModelList.get(0).getExperimentId());

        String fromDate = String.valueOf(Timestamp.valueOf("2010-10-10 09:00:00"));
        String toDate = String.valueOf(new Timestamp(System.currentTimeMillis()));

        filters.put(DBConstants.ExperimentSummary.FROM_DATE, fromDate);
        filters.put(DBConstants.ExperimentSummary.TO_DATE, toDate);
        experimentSummaryModelList = experimentSummaryRepository.
                searchExperiments(filters, -1, 0, null, null);
        assertTrue(experimentSummaryModelList.size() == 1);
        assertEquals(expertimentIdTwo, experimentSummaryModelList.get(0).getExperimentId());

        filters.remove(DBConstants.ExperimentSummary.FROM_DATE);
        filters.remove(DBConstants.ExperimentSummary.TO_DATE);

        List<String> accessibleExperimentIds = new ArrayList<>();
        accessibleExperimentIds.add(experimentIdOne);
        filters.put(DBConstants.Experiment.EXECUTION_ID, "executionIdOne");

        experimentSummaryModelList = experimentSummaryRepository.
                searchAllAccessibleExperiments(accessibleExperimentIds, filters, -1, 0, DBConstants.Experiment.CREATION_TIME, ResultOrderType.ASC);
        assertTrue(experimentSummaryModelList.size() == 1);
        assertEquals(experimentIdOne, experimentSummaryModelList.get(0).getExperimentId());

        filters = new HashMap<>();
        filters.put(DBConstants.Experiment.GATEWAY_ID, gatewayId);
        filters.put(DBConstants.Experiment.USER_NAME, "userTwo");
        filters.put(DBConstants.Experiment.RESOURCE_HOST_ID, "resourceHost");
        filters.put(DBConstants.Experiment.EXECUTION_ID, "executionIdTwo");
        filters.put(DBConstants.ExperimentSummary.FROM_DATE, fromDate);
        filters.put(DBConstants.ExperimentSummary.TO_DATE, toDate);

        ExperimentStatistics experimentStatistics = experimentSummaryRepository.getExperimentStatistics(filters);
        assertTrue(experimentStatistics.getAllExperimentCount() == 0);

        filters.remove(DBConstants.Experiment.RESOURCE_HOST_ID);

        experimentStatistics = experimentSummaryRepository.getExperimentStatistics(filters);
        assertTrue(experimentStatistics.getAllExperimentCount() == 1);
        assertEquals(experimentStatistics.getAllExperiments().get(0).getExperimentId(), expertimentIdTwo);

        filters.remove(DBConstants.Experiment.USER_NAME);
        filters.remove(DBConstants.Experiment.EXECUTION_ID);

        ExperimentStatus experimentStatusOne = new ExperimentStatus(ExperimentState.CREATED);
        String statusIdOne = experimentRepository.addExperimentStatus(experimentStatusOne, experimentIdOne);
        assertTrue(statusIdOne != null);

        ExperimentStatus experimentStatusTwo = new ExperimentStatus(ExperimentState.EXECUTING);
        String statusIdTwo = experimentRepository.addExperimentStatus(experimentStatusTwo, expertimentIdTwo);
        assertTrue(statusIdTwo != null);

        experimentStatistics = experimentSummaryRepository.getExperimentStatistics(filters);
        assertTrue(experimentStatistics.getAllExperimentCount() == 1);
        assertTrue(experimentStatistics.getRunningExperimentCount() == 1);
        assertEquals(expertimentIdTwo, experimentStatistics.getAllExperiments().get(0).getExperimentId());

        filters.remove(DBConstants.ExperimentSummary.FROM_DATE);
        filters.remove(DBConstants.ExperimentSummary.TO_DATE);

        experimentStatistics = experimentSummaryRepository.getExperimentStatistics(filters);
        assertTrue(experimentStatistics.getAllExperimentCount() == 2);
        assertTrue(experimentStatistics.getCreatedExperimentCount() == 1);

        experimentRepository.removeExperiment(experimentIdOne);
        experimentRepository.removeExperiment(expertimentIdTwo);

        gatewayRepository.removeGateway(gatewayId);
        projectRepository.removeProject(projectId);
    }

}
