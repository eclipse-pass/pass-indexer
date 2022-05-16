/*
 * Copyright 2017 Johns Hopkins University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dataconservancy.pass.indexer;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author apb@jhu.edu
 */
public class JmsClientTest {

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    @Test
    public void endToEndTest() throws Exception {
        final String queue = "test.queue";

        final CountDownLatch received = new CountDownLatch(1);
        try (final JmsClient one = new JmsClient(broker.createConnectionFactory())) {
            try (final JmsClient two = new JmsClient(broker.createConnectionFactory())) {

                one.listen(queue, m -> {
                    received.countDown();
                });

                final Session session = two.getSessionSupplier().get();
                final Message toSend = session.createTextMessage("hello");

                try (MessageProducer producer = session.createProducer(null)) {
                    producer.send(session.createQueue(queue), toSend);
                }

                assertTrue(received.await(10, TimeUnit.SECONDS));
            }
        }
    }
}
