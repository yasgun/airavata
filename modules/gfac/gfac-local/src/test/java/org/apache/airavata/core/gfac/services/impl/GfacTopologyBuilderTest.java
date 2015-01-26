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
package org.apache.airavata.core.gfac.services.impl;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;
import org.apache.airavata.gfac.local.AiravataRabbitMQSpout;
import org.apache.airavata.gfac.local.RandomSentenceSpout;
import org.apache.airavata.gfac.local.handler.LocalDirectorySetupBolt;
import org.apache.airavata.gfac.local.provider.impl.LocalProviderBolt;
import org.apache.airavata.gfac.local.utils.CustomStormDeclarator;
import org.apache.airavata.gfac.local.utils.MessageScheme;
import org.apache.airavata.registry.cpi.RegistryException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GfacTopologyBuilderTest {
    private final static Logger logger = LoggerFactory.getLogger(GfacTopologyBuilderTest.class);


    public static void main(String[] args) throws RegistryException, InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("gfac.submit")
                .prefetch(200)
                .requeueOnFail()
                .build();

//
        MessageScheme messageScheme = new MessageScheme();
        Declarator declarator = new CustomStormDeclarator("airavata_rabbitmq_exchange", "gfac.submit", "*");

        builder.setSpout("spout", new AiravataRabbitMQSpout(messageScheme,declarator), 5).addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(200);;
//        builder.setSpout("spout", new RandomSentenceSpout(), 5);

        builder.setBolt("directoryset", new LocalDirectorySetupBolt(), 8).shuffleGrouping("spout").setNumTasks(16);
        builder.setBolt("count", new LocalProviderBolt(), 12).shuffleGrouping("directoryset");
        Config conf = new Config();
        conf.setDebug(true);


        conf.setNumWorkers(3);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());

        Thread.sleep(10000000);

//        cluster.shutdown();
    }

    public void testTopology() throws RegistryException, InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("gfac.submit")
                .prefetch(200)
                .requeueOnFail()
                .build();

//
        MessageScheme messageScheme = new MessageScheme();
        Declarator declarator = new CustomStormDeclarator("airavata_rabbitmq_exchange", "gfac.submit", "*");

        builder.setSpout("spout", new AiravataRabbitMQSpout(messageScheme,declarator), 5).addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(200);;

        builder.setBolt("directoryset", new LocalDirectorySetupBolt(), 8).shuffleGrouping("spout").setNumTasks(16);
        builder.setBolt("count", new LocalProviderBolt(), 12).shuffleGrouping("directoryset");
        Config conf = new Config();
        conf.setDebug(true);


        conf.setNumWorkers(3);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());

        Thread.sleep(10000000);

//        cluster.shutdown();
    }
}
