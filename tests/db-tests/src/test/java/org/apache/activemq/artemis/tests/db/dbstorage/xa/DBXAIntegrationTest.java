/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.db.dbstorage.xa;

import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.db.dbstorage.statements.AbstractStatementTest;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.artemis.database.DatabaseProvider;
import org.apache.artemis.database.sql.SQLProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class DBXAIntegrationTest extends AbstractStatementTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @BeforeEach
   @Override
   public void setupTest() throws Exception {
      super.setupTest();
      configuration.addAddressConfiguration(new CoreAddressConfiguration().setName("DLQ").addRoutingType(RoutingType.ANYCAST).addQueueConfiguration(QueueConfiguration.of("DLQ").setRoutingType(RoutingType.ANYCAST)));
      configuration.addAddressConfiguration(new CoreAddressConfiguration().setName("ExpiryQueue").addRoutingType(RoutingType.ANYCAST).addQueueConfiguration(QueueConfiguration.of("ExpiryQueue").setRoutingType(RoutingType.ANYCAST)));
   }

   @Override
   protected boolean waitForBindings(ActiveMQServer server,
                                     String address,
                                     boolean local,
                                     int expectedBindingCount,
                                     int expectedConsumerCount,
                                     long timeout) throws Exception {
      return super.waitForBindings(server, address, local, expectedBindingCount, expectedConsumerCount, timeout);
   }

   @TestTemplate
   public void testSimpleXATXSend() throws Exception {

      ActiveMQServer server = createServer(true, configuration);
      server.start();

      runAfter(server::stop);

      final String QUEUE_NAME = "testXAQueue";
      final int NUM_MESSAGES = 10;

      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();
      SQLProvider sqlProvider = databaseProvider.getSqlProvider();

      Xid xid = newXID();

      // Step 1: send 10 messages within an XA transaction and prepare
      try (XAConnection xaConnection = ((XAConnectionFactory) factory).createXAConnection()) {
         XASession xaSession = xaConnection.createXASession();
         javax.jms.Queue queue = xaSession.createQueue(QUEUE_NAME);
         MessageProducer producer = xaSession.createProducer(queue);

         xaSession.getXAResource().start(xid, XAResource.TMNOFLAGS);
         for (int i = 0; i < NUM_MESSAGES; i++) {
            producer.send(xaSession.createTextMessage("message-" + i));
         }
         xaSession.getXAResource().end(xid, XAResource.TMSUCCESS);
         xaSession.getXAResource().prepare(xid);
      }

      // Step 2: verify prepare record and messages with TX_ID set
      try (Connection connection = databaseProvider.getConnection()) {
         Wait.assertEquals(1, () -> selectCount(connection, sqlProvider.getPrepTx()), 5000, 100);
         Wait.assertEquals(NUM_MESSAGES, () -> selectNumber(connection, "SELECT COUNT(*) FROM " + sqlProvider.getMessages() + " WHERE TX_ID IS NOT NULL"), 5000, 100);
      }

      // Step 2b: verify messages are not available for consumption before commit
      try (javax.jms.Connection connection = factory.createConnection()) {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(QUEUE_NAME);
         MessageConsumer consumer = session.createConsumer(queue);
         assertNull(consumer.receiveNoWait());
      }

      // Step 3: commit the XA transaction
      try (XAConnection xaConnection = ((XAConnectionFactory) factory).createXAConnection()) {
         XASession xaSession = xaConnection.createXASession();
         xaSession.getXAResource().commit(xid, false);
      }

      // Step 4: verify TX_ID is now null (messages committed) and prepare record removed
      try (Connection connection = databaseProvider.getConnection()) {
         Wait.assertEquals(0, () -> selectCount(connection, sqlProvider.getPrepTx()), 5000, 100);
         Wait.assertEquals(NUM_MESSAGES, () -> selectNumber(connection, "SELECT COUNT(*) FROM " + sqlProvider.getMessages() + " WHERE TX_ID IS NULL"), 5000, 100);
      }

      // Step 5: receive all messages and verify
      try (javax.jms.Connection connection = factory.createConnection()) {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         javax.jms.Queue queue = session.createQueue(QUEUE_NAME);
         MessageConsumer consumer = session.createConsumer(queue);
         for (int i = 0; i < NUM_MESSAGES; i++) {
            TextMessage msg = (TextMessage) consumer.receive(5000);
            assertNotNull(msg, "Expected message " + i);
         }
         assertNull(consumer.receiveNoWait());
         session.commit();
      }

      // Step 6: verify all messages consumed
      try (Connection connection = databaseProvider.getConnection()) {
         Wait.assertEquals(0, () -> selectCount(connection, sqlProvider.getMessages()), 5000, 100);
      }
   }
}