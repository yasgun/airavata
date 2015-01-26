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
package org.apache.airavata.gfac.local;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A simple RabbitMQ spout that emits an anchored tuple stream (on the default stream). This can be used with
 * Storm's guaranteed message processing.
 *
 * @author peter@latent.io
 */
public class AiravataRabbitMQSpout extends BaseRichSpout {

    private final MessageScheme scheme;
    private final Declarator declarator;

    private transient Logger logger;
    private transient RabbitMQConsumer consumer;
    private transient SpoutOutputCollector collector;

    public AiravataRabbitMQSpout(Scheme scheme) {
        this(MessageScheme.Builder.from(scheme), new Declarator.NoOp());
    }

    public AiravataRabbitMQSpout(Scheme scheme, Declarator declarator) {
        this(MessageScheme.Builder.from(scheme), declarator);
    }

    public AiravataRabbitMQSpout(MessageScheme scheme, Declarator declarator) {
        this.scheme = scheme;
        this.declarator = declarator;
    }

    @Override
    public void open(final Map config,
                     final TopologyContext context,
                     final SpoutOutputCollector spoutOutputCollector) {
        ConsumerConfig consumerConfig = ConsumerConfig.getFromStormConfig(config);

        ErrorReporter reporter = new ErrorReporter() {
            @Override
            public void reportError(Throwable error) {
                spoutOutputCollector.reportError(error);
            }
        };
        consumer = loadConsumer(declarator, reporter, consumerConfig);
        scheme.open(config, context);
        consumer.open();
        logger = LoggerFactory.getLogger(AiravataRabbitMQSpout.class);
        collector = spoutOutputCollector;
    }

    protected RabbitMQConsumer loadConsumer(Declarator declarator,
                                            ErrorReporter reporter,
                                            ConsumerConfig config) {
        return new RabbitMQConsumer(config.getConnectionConfig(),
                config.getPrefetchCount(),
                config.getQueueName(),
                config.isRequeueOnFail(),
                declarator,
                reporter);
    }

    @Override
    public void close() {
        consumer.close();
        scheme.close();
        super.close();
    }

    @Override
    public void nextTuple() {
        Message message;
        System.out.println("Waiting for messages !!!");
        while ((message = consumer.nextMessage()) != Message.NONE) {
            List<Object> tuple = extractTuple(message);
            if (!tuple.isEmpty()) {
                System.out.println(new String(message.getBody()));
                emit(tuple, message, collector);
            }
        }
    }

    protected List<Integer> emit(List<Object> tuple,
                                 Message message,
                                 SpoutOutputCollector spoutOutputCollector) {
        return spoutOutputCollector.emit(tuple, getDeliveryTag(message));
    }

    private List<Object> extractTuple(Message message) {
        long deliveryTag = getDeliveryTag(message);
        try {
            List<Object> tuple = scheme.deserialize(message);
            if (tuple != null && !tuple.isEmpty()) {
                return tuple;
            }
            String errorMsg = "Deserialization error for msgId " + deliveryTag;
            logger.warn(errorMsg);
            collector.reportError(new Exception(errorMsg));
        } catch (Exception e) {
            logger.warn("Deserialization error for msgId " + deliveryTag, e);
            collector.reportError(e);
        }
        // get the malformed message out of the way by dead-lettering (if dead-lettering is configured) and move on
        consumer.deadLetter(deliveryTag);
        return Collections.emptyList();
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Long) consumer.ack((Long) msgId);
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Long) consumer.fail((Long) msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(scheme.getOutputFields());
    }

    protected long getDeliveryTag(Message message) {
        return ((Message.DeliveredMessage) message).getDeliveryTag();
    }
}