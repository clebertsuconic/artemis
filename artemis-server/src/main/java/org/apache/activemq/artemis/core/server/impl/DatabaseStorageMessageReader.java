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
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.StorageMessageReader;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.utils.SizeAwareMetric;
import org.apache.artemis.database.data.MessageData;
import org.apache.artemis.database.queries.QueryUtil;
import org.apache.artemis.database.worker.BorrowedWorker;
import org.apache.artemis.database.worker.DataManager;
import org.apache.artemis.database.worker.DataWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseStorageMessageReader implements StorageMessageReader {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final long readIdleTimeoutMillis;

   final QueueImpl queue;

   Supplier<Integer> prefetchBytes;

   Supplier<Integer> prefetchMessages;

   volatile boolean scheduled;

   final DataManager dataManager;

   final DatabaseStorageManager databaseStorageManager;

   final SizeAwareMetric pagedSize = new SizeAwareMetric();


   @Override
   public long getReaderPaged() {
      return pagedSize.getElements();
   }

   // -- Borrowed worker state (accessed only from the queue's executor thread) --

   /** The currently borrowed worker, or null if none is active. */
   private BorrowedWorker borrowedWorker;

   /** The open ResultSet cursor from the borrowed worker, or null. */
   private ResultSet borrowedResultSet;

   /** True if the ResultSet was fully consumed (next() returned false). */
   private boolean resultSetExhausted;

   public DatabaseStorageMessageReader(QueueImpl queue, DatabaseStorageManager databaseStorageManager, PagingStore pagingStore) {
      this(queue, databaseStorageManager, pagingStore::getPrefetchPageMessages, pagingStore::getPrefetchPageBytes);
   }

   public DatabaseStorageMessageReader(QueueImpl queue, DatabaseStorageManager databaseStorageManager, Supplier<Integer> prefetchMessages, Supplier<Integer> prefetchBytes) {
      this.queue = queue;
      this.databaseStorageManager = databaseStorageManager;
      this.dataManager = databaseStorageManager.getDataManager();
      this.readIdleTimeoutMillis = databaseStorageManager.getDatabaseReadIdleTimeout();
      this.prefetchMessages = prefetchMessages;
      this.prefetchBytes = prefetchBytes;
   }

   @Override
   public void scheduleRead(boolean scheduleExpiry) {
      if (borrowedWorker != null && !borrowedWorker.isReturned()) {
         // We already have a borrowed worker with an open cursor — resume on the queue executor
         borrowedWorker.resume();
      } else {
         // Need to borrow a fresh worker — fetchMessages runs once the worker is available
         String description = "MessageDispatch queue = " + queue.getName() + ", address = " + queue.getAddress();
         dataManager.borrowWorker(queue.getExecutor(), readIdleTimeoutMillis, this::cleanupBorrowedWorker, this::fetchMessages, borrowed -> {
            borrowedWorker = borrowed;
            borrowedResultSet = null;
            resultSetExhausted = false;
            fetchMessages();
         }, description);
      }
   }

   /**
    * Core prefetch loop: opens a cursor if needed, reads a batch, delivers messages.
    * Called both on the initial borrow callback and on subsequent reads via
    * {@link BorrowedWorker#resume()}. Always runs on the queue's executor thread.
    */
   private void fetchMessages() {
      List<Message> receivedMessages = new ArrayList<>();
      try {
         if (borrowedResultSet == null || resultSetExhausted) {
            // Need to open a new cursor (first call, or previous cursor was exhausted)
            if (!openCursor()) {
               return;
            }
         }

         readBatch(borrowedWorker.getWorker(), borrowedResultSet, receivedMessages);
         if (borrowedWorker != null && !borrowedWorker.isReturned()) {
            borrowedWorker.renewLease();
         }
      } catch (Throwable e) {
         logger.warn("Error during prefetch for queue {}: {}", queue.getName(), e.getMessage(), e);
         reconnectReturnAndReschedule(e);
      } finally {
         deliverMessages(receivedMessages);
      }
   }

   public long getPagedMessages() {
      return pagedSize.getElements();
   }

   public long getPagedBytes() {
      return pagedSize.getSize();
   }

   /**
    * Opens a new query cursor on the borrowed worker. On failure, the worker is
    * reconnected, returned to the pool, and a fresh read is scheduled.
    *
    * @return true if the cursor was opened successfully, false if the worker was returned
    */
   private boolean openCursor() {
      try {
         long start = System.currentTimeMillis();
         borrowedResultSet = borrowedWorker.getWorker().pendingDeliveryQueryForUpdate.execute(queue.getID());
         long end = System.currentTimeMillis();
         logger.info("took {} milliseconds to start returning results", (end - start));
         resultSetExhausted = false;
         return true;
      } catch (Throwable e) {
         logger.warn("Failed to execute prefetch query for queue {}: {}", queue.getName(), e.getMessage(), e);
         reconnectReturnAndReschedule(e);
         return false;
      }
   }

   /**
    * Reads up to one batch of messages from the ResultSet cursor. Commits the
    * pending delivery updates after each batch. If the ResultSet is exhausted
    * or the pool demands the connection back, the borrowed worker is returned.
    */
   private void readBatch(DataWorker worker, ResultSet resultSet, List<Message> messageList) throws Exception {
      int prefetchBytesValue = prefetchBytes.get();
      int prefetchMessagesValue = prefetchMessages.get();

      if (prefetchMessagesValue <= 0 && prefetchBytesValue <= 0) {
         prefetchBytesValue = 1000;
      }

      int messagesRead = 0;
      int bytesRead = 0;
      boolean cursorHasMore = true;

      while (queue.needsDepage()) {
         // Check the cooperative cancellation flag before each row
         if (borrowedWorker != null && borrowedWorker.needConnectionBack) {
            logger.debug("Pool demands connection back, aborting prefetch for queue {}", queue.getName());
            break;
         }

         if (!resultSet.next()) {
            cursorHasMore = false;
            break;
         }

         MessageData messageData = QueryUtil.readMessageData(resultSet, 1, 2, 3);
         messagesRead++;
         bytesRead += messageData.memoryEstimate;

         Message message = DatabaseStorageManager.decodeMessage(messageData);
         messageList.add(message);

         worker.pendingDeliveryQueryForUpdate.updateDelivery(queue.getID(), messageData.messageID);
         if (prefetchMessagesValue > 0 && messagesRead >= prefetchMessagesValue ||
            prefetchBytesValue >= 0 && bytesRead >= prefetchBytesValue) {
            break;
         }
      }

      // Flush and commit the delivery updates for this batch
      worker.pendingDeliveryQueryForUpdate.flush();
      try {
         worker.commit();
      } catch (Throwable e) {
         logger.warn("Commit failed during prefetch for queue {}: {}", queue.getName(), e.getMessage(), e);
         dataManager.criticalError(e);
         return;
      }

      if (!cursorHasMore) {
         resultSetExhausted = true;
         returnBorrowedWorkerNow();
      } else if (borrowedWorker != null && borrowedWorker.needConnectionBack) {
         returnBorrowedWorkerNow();
      }
   }

   /**
    * Cleanup action called when the borrowed worker is returned (on lease expiry,
    * explicit return, or demand-back). Closes the open ResultSet if any.
    * This is called from the queue's executor thread via BorrowedWorker.
    */
   private void cleanupBorrowedWorker() {
      if (borrowedResultSet != null) {
         try {
            borrowedResultSet.close();
         } catch (SQLException e) {
            logger.debug("Error closing borrowed ResultSet for queue {}: {}", queue.getName(), e.getMessage(), e);
         }
         borrowedResultSet = null;
      }
      resultSetExhausted = false;
      borrowedWorker = null;
   }

   /**
    * Explicitly return the borrowed worker now (e.g. because the cursor is exhausted
    * or an error occurred). The cleanup action runs as part of returnWorker().
    */
   private void returnBorrowedWorkerNow() {
      if (borrowedWorker != null && !borrowedWorker.isReturned()) {
         borrowedWorker.returnWorker();
      }
      // cleanupBorrowedWorker() is called by returnWorker() via the cleanup action,
      // but clear our references in case it wasn't set
      borrowedWorker = null;
      borrowedResultSet = null;
      resultSetExhausted = false;
   }

   /**
    * On a connection failure: reconnect the worker (so it returns to the pool healthy),
    * then schedule a fresh read that will borrow a new worker.
    */
   private void reconnectReturnAndReschedule(Throwable cause) {
      if (borrowedWorker != null && !borrowedWorker.isReturned()) {
         java.sql.SQLException reconnectError = borrowedWorker.reconnect(cause);
         if (reconnectError != null) {
            logger.warn("Failed to reconnect worker for queue {}: {}", queue.getName(), reconnectError.getMessage(), reconnectError);
            dataManager.criticalError(reconnectError);
         }
         returnBorrowedWorkerNow();
      }
      scheduleRead(false);
   }

   private void deliverMessages(List<Message> messageList) {
      for (Message m : messageList) {
         MessageReference reference = new MessageReferenceImpl(m, queue);
         pagedSize.simpleAdd(-1, reference.getMessage().getMemoryEstimate());
         queue.refUp(reference);
         queue.durableUp(m);
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
      if (pagedSize.getElements() < 0) {
         logger.warn("StorageReader {} with negative values at {}", queue.getName(), pagedSize.getElements());
      }
      if (pagedSize.getElements() <= 0) {
         logger.info("nothing to read.. give up");
         return;
      }
      boolean needsDepage = pagedSize.getElements() > 0 && queue.needsDepage();
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

   public void addPendingAfterStorage(long elements, long size, Transaction tx) {
      if (databaseStorageManager == null) {
         // it may happen on tests, in that case we are not testing for this interaction, so it's okay to return and ignore it
         return;
      }
      if (tx != null) {
         PendingDeliveryUpdate pendingDeliveryUpdate = tx.getOrCreateOperation(TransactionPropertyIndexes.PENDING_DELIVERY_UPDATE, PendingDeliveryUpdate::new);
         pendingDeliveryUpdate.addSize(queue, elements, size);
      } else {
         databaseStorageManager.afterCompleteOperations(new IOCallback() {
            @Override
            public void done() {
               StorageMessageReader reader = queue.getStorageMessageReader();
               if (reader != null) {
                  reader.addPending(elements, size);
               }
            }

            @Override
            public void onError(int errorCode, String errorMessage) {

            }
         });
      }
   }

   @Override
   public void addPending(long elements, long size) {
      pagedSize.simpleAdd(elements, size);
   }


   protected static class PendingDeliveryUpdate extends TransactionOperationAbstract {

      protected HashMap<Queue, SizeAwareMetric> pendingUpdates = new HashMap<>();


      public void addSize(Queue queue, long elements, long size) {
         SizeAwareMetric updateSizeAware = pendingUpdates.computeIfAbsent(queue, k -> new SizeAwareMetric());
         updateSizeAware.simpleAdd(elements, size);
      }

      @Override
      public void afterCommit(Transaction tx) {
         pendingUpdates.forEach(this::updateQueue);
      }

      private void updateQueue(Queue queue, SizeAwareMetric metric) {
         queue.getStorageMessageReader().addPending(metric.getElements(), metric.getSize());
      }
   }


}
