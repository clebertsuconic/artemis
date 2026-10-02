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
package org.apache.activemq.artemis.tests.db.dbstorage.statements;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.artemis.database.DatabaseProvider;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class LargeMessagesStatementTest extends AbstractStatementTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   // LargeMessageJournalTest was written to give me a comparison to this test. I am keeping the journal equivalent as it's a good test
   // in the future if you need to compare the two paths refer to LargeMessageJournalTest
   @TestTemplate
   public void testCoreLargeMessage() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration, criticalAnalyzer, executorFactory, executorFactory, scheduledExecutorService, executorService, null);
      databaseStorageManager.start();
      try {
         DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

         Connection connection = databaseProvider.getConnection();
         runAfter(connection::close);

         OperationContext context = databaseStorageManager.getContext();
         runAfter(OperationContextImpl::clearContext);

         databaseStorageManager.generateID(); // skip the zero

         long id = databaseStorageManager.generateID();
         LargeServerMessage largeMessage = databaseStorageManager.createCoreLargeMessage();
         databaseStorageManager.onLargeMessageCreate(id, largeMessage);
         largeMessage.setDurable(true);

         byte[] bodyBytes = RandomUtil.randomBytes(100 * 1024);
         largeMessage.addBytes(bodyBytes);

         long memoryEstimate = largeMessage.getMessage().getMemoryEstimate();
         logger.info("Memory Estimate {}", memoryEstimate);

         databaseStorageManager.storeMessage(largeMessage.getMessage());

         assertTrue(context.waitCompletion(5000));

         assertEquals(1, selectCount(connection, databaseProvider.getSqlProvider().getMessages()));

         logger.debug("Large message stored with id={}", id);
      } finally {
         databaseStorageManager.stop();
      }
   }
}
