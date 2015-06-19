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
CREATE TABLE GATEWAY
(
        GATEWAY_ID VARCHAR(255),
        GATEWAY_NAME VARCHAR(255),
	      DOMAIN VARCHAR(255),
	      EMAIL_ADDRESS VARCHAR(255),
        PRIMARY KEY (GATEWAY_ID)
);

CREATE TABLE USER
(
        USER_NAME VARCHAR(255),
        PASSWORD VARCHAR(255),
        PRIMARY KEY(USER_NAME)
);

CREATE TABLE GATEWAY_WORKER
(
        GATEWAY_ID VARCHAR(255),
        USER_NAME VARCHAR(255),
        PRIMARY KEY (GATEWAY_ID, USER_NAME),
        FOREIGN KEY (GATEWAY_ID) REFERENCES GATEWAY(GATEWAY_ID) ON DELETE CASCADE,
        FOREIGN KEY (USER_NAME) REFERENCES USER(USER_NAME) ON DELETE CASCADE
);

CREATE TABLE PROJECT
(
         GATEWAY_ID VARCHAR(255),
         USER_NAME VARCHAR(255),
         PROJECT_NAME VARCHAR(255) NOT NULL,
         PROJECT_ID VARCHAR(255),
         DESCRIPTION VARCHAR(255),
         CREATION_TIME TIMESTAMP DEFAULT NOW(),
         PRIMARY KEY (PROJECT_ID),
         FOREIGN KEY (GATEWAY_ID) REFERENCES GATEWAY(GATEWAY_ID) ON DELETE CASCADE,
         FOREIGN KEY (USER_NAME) REFERENCES USER(USER_NAME) ON DELETE CASCADE
);

CREATE TABLE PROJECT_USER
(
    PROJECT_ID VARCHAR(255),
    USER_NAME VARCHAR(255),
    PRIMARY KEY (PROJECT_ID,USER_NAME),
    FOREIGN KEY (PROJECT_ID) REFERENCES PROJECT(PROJECT_ID) ON DELETE CASCADE,
    FOREIGN KEY (USER_NAME) REFERENCES USER(USER_NAME) ON DELETE CASCADE
);

CREATE TABLE EXPERIMENT (
  EXPERIMENT_ID varchar(255),
  PROJECT_ID varchar(255),
  EXPERIMENT_TYPE varchar(255),
  USER_NAME varchar(255),
  APPLICATION_ID varchar(255),
  EXPERIMENT_NAME varchar(255),
  CREATION_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  DESCRIPTION varchar(255),
  EXECUTION_ID varchar(255),
  GATEWAY_EXECUTION_ID varchar(255),
  ENABLE_EMAIL_NOTIFICATION BOOLEAN,
  EMAIL_ADDRESSES CLOB,
  PRIMARY KEY (EXPERIMENT_ID),
  FOREIGN KEY (USER_NAME) REFERENCES USER(USER_NAME) ON DELETE CASCADE,
  FOREIGN KEY (PROJECT_ID) REFERENCES PROJECT(PROJECT_ID) ON DELETE CASCADE
);


CREATE TABLE EXPERIMENT_INPUT
(
    EXPERIMENT_ID varchar(255),
    INPUT_NAME varchar(255),
    INPUT_VALUE CLOB,
    DATA_TYPE varchar(255),
    APPLICATION_ARGUMENT varchar(255),
    STANDARD_INPUT BOOLEAN,
    USER_FRIENDLY_DESCRIPTION varchar(255),
    METADATA varchar(255),
    INPUT_ORDER int(11),
    IS_REQUIRED BOOLEAN,
    REQUIRED_TO_ADDED_TO_CMD BOOLEAN,
    DATA_STAGED BOOLEAN,
    PRIMARY KEY(EXPERIMENT_ID,INPUT_NAME),
    FOREIGN KEY (EXPERIMENT_ID) REFERENCES EXPERIMENT(EXPERIMENT_ID) ON DELETE CASCADE
);

CREATE TABLE EXPERIMENT_OUTPUT
(
    EXPERIMENT_ID varchar(255),
    OUTPUT_NAME varchar(255),
    DATA_TYPE varchar(255),
    APPLICATION_ARGUMENT varchar(255),
    IS_REQUIRED BOOLEAN,
    REQUIRED_TO_ADDED_TO_CMD BOOLEAN,
    DATA_MOVEMENT BOOLEAN,
    LOCATION varchar(255),
    SEARCH_QUERY varchar(255),
    PRIMARY KEY(EXPERIMENT_ID,OUTPUT_NAME),
    FOREIGN KEY (EXPERIMENT_ID) REFERENCES EXPERIMENT(EXPERIMENT_ID) ON DELETE CASCADE
);


CREATE TABLE EXPERIMENT_STATUS (
  EXPERIMENT_ID varchar(255),
  STATE varchar(255),
  TIME_OF_STATE_CHANGE timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  REASON varchar(255),
  PRIMARY KEY (EXPERIMENT_ID),
  FOREIGN KEY (EXPERIMENT_ID) REFERENCES EXPERIMENT(EXPERIMENT_ID) ON DELETE CASCADE
);


CREATE TABLE EXPERIMENT_ERROR (
  ERROR_ID int(11) NOT NULL AUTO_INCREMENT,
  EXPERIMENT_ID varchar(255) NOT NULL,
  CREATION_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ACTUAL_ERROR_MESSAGE CLOB,
  USER_FRIENDLY_MESSAGE CLOB,
  TRANSIENT_OR_PERSISTENT BOOLEAN,
  ROOT_CAUSE_ERROR_ID_LIST CLOB,
  PRIMARY KEY (ERROR_ID),
  FOREIGN KEY (EXPERIMENT_ID) REFERENCES EXPERIMENT(EXPERIMENT_ID) ON DELETE CASCADE
);

CREATE TABLE USER_CONFIGURATION_DATA (
  EXPERIMENT_ID varchar(255),
  AIRAVATA_AUTO_SCHEDULE BOOLEAN,
  OVERRIDE_MANUAL_SCHEDULED_PARAMS BOOLEAN,
  SHARE_EXPERIMENT_PUBLICALLY BOOLEAN,
  THROTTLE_RESOURCES BOOLEAN,
  USER_DN varchar(255),
  GENERATE_CERT BOOLEAN,
  RESOURCE_HOST_ID varchar(255),
  TOTAL_CPU_COUNT int(11),
  NODE_COUNT int(11),
  NUMBER_OF_THREADS int(11),
  QUEUE_NAME varchar(255),
  WALL_TIME_LIMIT int(11),
  TOTAL_PHYSICAL_MEMORY int(11),
  PRIMARY KEY (EXPERIMENT_ID),
  FOREIGN KEY (EXPERIMENT_ID) REFERENCES EXPERIMENT(EXPERIMENT_ID) ON DELETE CASCADE
);

CREATE VIEW EXPERIMENT_SUMMARY AS
  select E.EXPERIMENT_ID AS EXPERIMENT_ID, E.PROJECT_ID AS PROJECT_ID,
  E.USER_NAME AS USER_NAME, E.APPLICATION_ID AS APPLICATION_ID, E.EXPERIMENT_NAME AS EXPERIMENT_NAME,
  E.CREATION_TIME AS CREATION_TIME, E.DESCRIPTION AS DESCRIPTION, ES.STATE AS STATE, UD.RESOURCE_HOST_ID
  AS RESOURCE_HOST_ID, ES.TIME_OF_STATE_CHANGE AS TIME_OF_STATE_CHANGE
    from ((EXPERIMENT E left join EXPERIMENT_STATUS ES on((E.EXPERIMENT_ID = ES.EXPERIMENT_ID)))
    left join USER_CONFIGURATION_DATA UD on((E.EXPERIMENT_ID = UD.EXPERIMENT_ID))) where 1;

CREATE TABLE PROCESS (
  PROCESS_ID varchar(255),
  EXPERIMENT_ID varchar(255),
  CREATION_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  LAST_UPDATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PROCESS_DETAIL CLOB,
  APPLICATION_INTERFACE_ID varchar(255),
  TASK_DAG varchar(255),
  PRIMARY KEY (PROCESS_ID),
  FOREIGN KEY (EXPERIMENT_ID) REFERENCES EXPERIMENT(EXPERIMENT_ID) ON DELETE CASCADE
);

CREATE TABLE PROCESS_INPUT
(
    PROCESS_ID varchar(255),
    INPUT_NAME varchar(255),
    INPUT_VALUE CLOB,
    DATA_TYPE varchar(255),
    APPLICATION_ARGUMENT varchar(255),
    STANDARD_INPUT BOOLEAN,
    USER_FRIENDLY_DESCRIPTION varchar(255),
    METADATA varchar(255),
    INPUT_ORDER int(11),
    IS_REQUIRED BOOLEAN,
    REQUIRED_TO_ADDED_TO_CMD BOOLEAN,
    DATA_STAGED BOOLEAN,
    PRIMARY KEY(PROCESS_ID,INPUT_NAME),
    FOREIGN KEY (PROCESS_ID) REFERENCES PROCESS(PROCESS_ID) ON DELETE CASCADE
);

CREATE TABLE PROCESS_OUTPUT
(
    PROCESS_ID varchar(255),
    OUTPUT_NAME varchar(255),
    DATA_TYPE varchar(255),
    APPLICATION_ARGUMENT varchar(255),
    IS_REQUIRED BOOLEAN,
    REQUIRED_TO_ADDED_TO_CMD BOOLEAN,
    DATA_MOVEMENT BOOLEAN,
    LOCATION varchar(255),
    SEARCH_QUERY varchar(255),
    PRIMARY KEY(PROCESS_ID,OUTPUT_NAME),
    FOREIGN KEY (PROCESS_ID) REFERENCES PROCESS(PROCESS_ID) ON DELETE CASCADE
);


CREATE TABLE PROCESS_STATUS (
  PROCESS_ID varchar(255),
  STATE varchar(255),
  TIME_OF_STATE_CHANGE timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  REASON varchar(255),
  PRIMARY KEY (PROCESS_ID),
  FOREIGN KEY (PROCESS_ID) REFERENCES PROCESS(PROCESS_ID) ON DELETE CASCADE
);


CREATE TABLE PROCESS_ERROR (
  ERROR_ID int(11) NOT NULL AUTO_INCREMENT,
  PROCESS_ID varchar(255) NOT NULL,
  CREATION_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ACTUAL_ERROR_MESSAGE CLOB,
  USER_FRIENDLY_MESSAGE CLOB,
  TRANSIENT_OR_PERSISTENT BOOLEAN,
  ROOT_CAUSE_ERROR_ID_LIST CLOB,
  PRIMARY KEY (ERROR_ID),
  FOREIGN KEY (PROCESS_ID) REFERENCES PROCESS(PROCESS_ID) ON DELETE CASCADE
);

CREATE TABLE PROCESS_RESOURCE_SCHEDULE (
  PROCESS_ID varchar(255),
  RESOURCE_HOST_ID varchar(255),
  TOTAL_CPU_COUNT int(11),
  NODE_COUNT int(11),
  NUMBER_OF_THREADS int(11),
  QUEUE_NAME varchar(255),
  WALL_TIME_LIMIT int(11),
  TOTAL_PHYSICAL_MEMORY int(11),
  PRIMARY KEY (PROCESS_ID)
);

CREATE TABLE TASK (
  TASK_ID varchar(255),
  TASK_TYPE varchar(255),
  PARENT_PROCESS_ID varchar(255),
  CREATION_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  LAST_UPDATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  TASK_DETAIL CLOB,
  TASK_INTERNAL_STORE CHAR,
  PRIMARY KEY (TASK_ID),
  FOREIGN KEY (PARENT_PROCESS_ID) REFERENCES PROCESS(PROCESS_ID) ON DELETE CASCADE
);

CREATE TABLE TASK_STATUS (
  TASK_ID varchar(255),
  STATE varchar(255),
  TIME_OF_STATE_CHANGE timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  REASON varchar(255),
  PRIMARY KEY (TASK_ID),
  FOREIGN KEY (TASK_ID) REFERENCES TASK(TASK_ID) ON DELETE CASCADE
);


CREATE TABLE EXPERIMENT_ERROR (
  ERROR_ID int(11) NOT NULL AUTO_INCREMENT,
  TASK_ID varchar(255) NOT NULL,
  CREATION_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ACTUAL_ERROR_MESSAGE CLOB,
  USER_FRIENDLY_MESSAGE CLOB,
  TRANSIENT_OR_PERSISTENT BOOLEAN,
  ROOT_CAUSE_ERROR_ID_LIST CLOB,
  PRIMARY KEY (ERROR_ID),
  FOREIGN KEY (TASK_ID) REFERENCES TASK(TASK_ID) ON DELETE CASCADE
);