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
package org.apache.airavata.gfac.local.utils;

import com.rabbitmq.client.Channel;
import io.latent.storm.rabbitmq.Declarator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomStormDeclarator implements Declarator {
    private final static Logger logger = LoggerFactory.getLogger(CustomStormDeclarator.class);

    private final String exchange;
    private final String queue;
    private final String routingKey;

    public CustomStormDeclarator(String exchange, String queue) {
        this(exchange, queue, "");
    }

    public CustomStormDeclarator(String exchange, String queue, String routingKey) {
        this.exchange = exchange;
        this.queue = queue;
        this.routingKey = routingKey;
    }

    @Override
    public void execute(Channel channel) {
        // you're given a RabbitMQ Channel so you're free to wire up your exchange/queue bindings as you see fit
        try {
            Map<String, Object> args = new HashMap<String,Object>();
            channel.queueDeclare(queue, true, false, false, args);
            channel.exchangeDeclare(exchange, "topic", true);
            channel.queueBind(queue, exchange, routingKey);
        } catch (IOException e) {
            throw new RuntimeException("Error executing rabbitmq declarations.", e);
        }
    }
}
