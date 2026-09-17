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
package org.apache.activemq.artemis.core.server.impl;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.StorageMessageReader;
import org.apache.artemis.database.data.MessageData;
import org.apache.artemis.database.queries.QueryUtil;
import org.apache.artemis.database.worker.DataWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseStorageMessageReader implements StorageMessageReader {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final QueueImpl queue;

   final PagingStore pagingStore;

   final DatabaseStorageManager databaseStorageManager;

   public DatabaseStorageMessageReader(QueueImpl queue, DatabaseStorageManager databaseStorageManager, PagingStore pagingStore) {
      this.queue = queue;
      this.databaseStorageManager = databaseStorageManager;
      this.pagingStore = pagingStore;
   }

   @Override
   public void scheduleRead(boolean scheduleExpiry) {
      logger.info("Scheduling read...", new Exception());
      List<Message> receivedMessages = new ArrayList<>();
      databaseStorageManager.getDataManager().executeQuery(queue.getExecutor(), w -> this.executePrefetch(w, receivedMessages), () -> deliverMessages(receivedMessages));
   }

   private void executePrefetch(DataWorker worker, List<Message> messageList) throws SQLException {
      try (ResultSet resultSet = worker.pendingDeliveryQueryForUpdate.execute(queue.getID())) {
         final int prefetchBytes = pagingStore.getPrefetchPageBytes();
         int prefetchMessages = pagingStore.getPrefetchPageMessages();

         if (prefetchMessages <= 0 && prefetchMessages <= 0) {
            prefetchMessages = 1000;
         }

         int messagesRead = 0;
         int bytesRead = 0;
         while (queue.needsDepage() && resultSet.next()) {
            MessageData messageData = QueryUtil.readMessageData(resultSet, 1, 2, 3);
            messagesRead++;
            bytesRead += messageData.memoryEstimate;

            if (prefetchMessages > 0 && messagesRead >= prefetchMessages ||
                prefetchBytes > 0 && bytesRead >= bytesRead) {
                  break;
            }
            messageList.add(DatabaseStorageManager.decodeMessage(messageData));

            worker.pendingDeliveryQueryForUpdate.updateDelivery(queue.getID(), messageData.messageID);
         }
         worker.pendingDeliveryQueryForUpdate.flush();
      }
   }

   private void deliverMessages(List<Message> messageList) {
      for (Message m : messageList) {
         MessageReference reference = new MessageReferenceImpl(m, queue);
         queue.addSorted(reference, false);
      }
      queue.deliverAsync();
   }

   @Override
   public void checkRead() {
      new Exception("checkRead").printStackTrace();
      if (queue.needsDepage()) {
         scheduleRead(false);
      }
   }

   @Override
   public void lock() {
   }

   @Override
   public void unlock() {
   }

   @Override
   public boolean allowDirectDelivery() {
      return false;
   }

   @Override
   public int iterateMessages(String operationName, int flushLimit, boolean separatePageIterator, QueueImpl.QueueIterateAction messageAction, int count) throws Exception {
      return count;
   }
}
