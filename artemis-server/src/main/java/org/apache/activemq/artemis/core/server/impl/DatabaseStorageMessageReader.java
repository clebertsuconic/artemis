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
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.StorageMessageReader;
import org.apache.activemq.artemis.utils.SizeAwareMetric;
import org.apache.artemis.database.data.MessageData;
import org.apache.artemis.database.queries.QueryUtil;
import org.apache.artemis.database.worker.DataManager;
import org.apache.artemis.database.worker.DataWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseStorageMessageReader implements StorageMessageReader {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final QueueImpl queue;

   Supplier<Integer> prefetchBytes;

   Supplier<Integer> prefetchMessages;

   volatile boolean scheduled;

   final DataManager dataManager;

   final SizeAwareMetric pagedSize = new SizeAwareMetric();

   public DatabaseStorageMessageReader(QueueImpl queue, DatabaseStorageManager databaseStorageManager, PagingStore pagingStore) {
      this(queue, databaseStorageManager.getDataManager(), pagingStore::getPrefetchPageMessages, pagingStore::getPrefetchPageBytes);
   }

   public DatabaseStorageMessageReader(QueueImpl queue, DataManager dataManager, Supplier<Integer> prefetchMessages, Supplier<Integer> prefetchBytes) {
      this.queue = queue;
      this.dataManager = dataManager;
      this.prefetchMessages = prefetchMessages;
      this.prefetchBytes = prefetchBytes;
   }

   @Override
   public void scheduleRead(boolean scheduleExpiry) {
      List<Message> receivedMessages = new ArrayList<>();
      dataManager.executeQuery(queue.getExecutor(), w -> this.executePrefetch(w, receivedMessages), () -> deliverMessages(receivedMessages));
   }

   public long getPagedMessages() {
      return pagedSize.getElements();
   }

   public long getPagedBytes() {
      return pagedSize.getSize();
   }

   private void executePrefetch(DataWorker worker, List<Message> messageList) throws SQLException {
      try (ResultSet resultSet = worker.pendingDeliveryQueryForUpdate.execute(queue.getID())) {
         int prefetchBytesValue = prefetchBytes.get();
         int prefetchMessagesValue = prefetchMessages.get();

         if (prefetchMessagesValue <= 0 && prefetchBytesValue <= 0) {
            prefetchBytesValue = 1000;
         }

         int messagesRead = 0;
         int bytesRead = 0;
         while (queue.needsDepage() && resultSet.next()) {
            MessageData messageData = QueryUtil.readMessageData(resultSet, 1, 2, 3);
            messagesRead++;
            bytesRead += messageData.memoryEstimate;

            Message message = DatabaseStorageManager.decodeMessage(messageData);
            logger.info("Prefetching message {}", message);
            messageList.add(message);

            worker.pendingDeliveryQueryForUpdate.updateDelivery(queue.getID(), messageData.messageID);
            if (prefetchMessagesValue > 0 && messagesRead >= prefetchMessagesValue ||
               prefetchBytesValue >= 0 && bytesRead >= prefetchBytesValue) {
               break;
            }
         }
         worker.pendingDeliveryQueryForUpdate.flush();
      }
   }

   public void reloadPage(long messages, long size) {
      pagedSize.reloadValue(messages, size);
   }

   private void deliverMessages(List<Message> messageList) {
      for (Message m : messageList) {
         MessageReference reference = new MessageReferenceImpl(m, queue);
         queue.refUp(reference);
         queue.addSorted(reference, false);
      }
      queue.deliverAsync();
      // this call is always performed from a single thread call. no need to synchronized here
      scheduled = false;
   }

   @Override
   public void checkRead() {
      if (queue.isPaused()) {
         return;
      }
      boolean needsDepage = queue.needsDepage();
      synchronized (this) {
         if (!scheduled && !queue.isPaused() && needsDepage) {
            scheduled = true;
            scheduleRead(false);
         }
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
