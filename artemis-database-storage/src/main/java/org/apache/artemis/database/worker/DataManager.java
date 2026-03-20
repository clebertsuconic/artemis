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

package org.apache.artemis.database.worker;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.StorageTX;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.utils.TableOut;
import org.apache.artemis.database.ActiveMQDatabaseLogger;
import org.apache.artemis.database.ActiveMQDirectDBBundle;
import org.apache.artemis.database.DatabaseProvider;
import org.apache.artemis.database.DatabaseStoreTX;
import org.apache.artemis.database.data.AddressData;
import org.apache.artemis.database.data.DBData;
import org.apache.artemis.database.data.DeleteAddressData;
import org.apache.artemis.database.data.DeleteAllPageRefData;
import org.apache.artemis.database.data.DeleteGenericData;
import org.apache.artemis.database.data.DeleteMessageData;
import org.apache.artemis.database.data.DeletePageData;
import org.apache.artemis.database.data.DeletePageRefData;
import org.apache.artemis.database.data.DeleteQueueData;
import org.apache.artemis.database.data.DeleteReferenceData;
import org.apache.artemis.database.data.GenericData;
import org.apache.artemis.database.data.MessageData;
import org.apache.artemis.database.data.MessageReferenceData;
import org.apache.artemis.database.data.PageData;
import org.apache.artemis.database.data.PageRefData;
import org.apache.artemis.database.data.QueueData;
import org.apache.artemis.database.data.TXDone;
import org.apache.artemis.database.data.UpdateGenericData;
import org.apache.artemis.database.data.UpdateQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.artemis.database.worker.DataManagerUtil.ioCompletions;

public class DataManager extends ActiveMQScheduledComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final DatabaseProvider databaseProvider;
   final int batchSize;
   final int maxReadConnections;
   final IntSupplier maxRetriesSupplier;
   final LongSupplier retryIntervalMillisSupplier;
   final Consumer<Throwable> criticalErrorListener;
   final Executor executorService;
   final ScheduledExecutorService scheduledExecutorService;

   List<DataWorker> allWorkers;

   LinkedBlockingDeque<DataWorker> workers;
   final ConcurrentLinkedQueue<BaseInterceptor> scheduledQueries = new ConcurrentLinkedQueue<>();
   final Set<BorrowedWorker> borrowedWorkers = ConcurrentHashMap.newKeySet();

   final Semaphore dataLock = new Semaphore(1);
   final ArrayList<DBData> pendingData = new ArrayList<>();

   public MessageReferenceData newReferenceTask(long messageID,
                                                long queueID,
                                                boolean pendingDelivery,
                                                Long txID,
                                                IOCompletion context) {
      return new MessageReferenceData(messageID, queueID, pendingDelivery, txID, context);
   }

   public MessageData newMessageTask(long messageID,
                                     Supplier<ActiveMQBuffer> messageBufferSupplier,
                                     Long txID,
                                     int memoryEstimate,
                                     IOCompletion context) {
      return new MessageData(messageID, messageBufferSupplier, txID, memoryEstimate, context);
   }

   public DataManager(ScheduledExecutorService scheduledExecutorService,
                      Executor executor,
                      Executor executorService,
                      long flushTimeNanos,
                      DatabaseProvider databaseProvider,
                      int batchSize,
                      int numberOfConnections,
                      int maxReadConnections,
                      IntSupplier maxRetriesSupplier,
                      LongSupplier retryIntervalMillisSupplier,
                      Consumer<Throwable> criticalErrorListener) throws SQLException {
      super(scheduledExecutorService, executor, 0, flushTimeNanos, TimeUnit.NANOSECONDS, true);

      this.scheduledExecutorService = scheduledExecutorService;
      this.maxReadConnections = maxReadConnections >= 0 ? maxReadConnections : numberOfConnections / 2;
      this.maxRetriesSupplier = maxRetriesSupplier;
      this.retryIntervalMillisSupplier = retryIntervalMillisSupplier;
      this.criticalErrorListener = criticalErrorListener;
      allWorkers = new ArrayList<>();
      workers = new LinkedBlockingDeque<>();
      for (int i = 0; i < numberOfConnections; i++) {
         DataWorker worker = new DataWorker(this, databaseProvider, batchSize, "worker " + i);
         allWorkers.add(worker);
         workers.offer(worker);
      }

      this.executorService = executorService;

      logger.info("FlushTime {}", flushTimeNanos);
      this.databaseProvider = databaseProvider;
      this.batchSize = batchSize;
      init();
   }

   public void init() throws SQLException {
   }

   public void close() {
      // Return all borrowed workers before closing
      for (BorrowedWorker borrowed : borrowedWorkers) {
         borrowed.returnWorker();
      }
      borrowedWorkers.clear();
      allWorkers.forEach(DataWorker::close);
      allWorkers.clear();
      workers.clear();
   }

   public int getMaxRetries() {
      return maxRetriesSupplier.getAsInt();
   }

   public long getRetryIntervalMillis() {
      return retryIntervalMillisSupplier.getAsLong();
   }

   public void criticalError(Throwable error) {
      criticalErrorListener.accept(error);
   }

   public void storeTX(StorageTX storageTX) {
      flushData(castTX(storageTX).dataList, storageTX);
   }

   /** tryLock uses a semaphore in the background.
    * You're supposed to use it in quick operations only. Most of the lock is not needed since this is single threaded.
    * Only call releaseLock if acquirelock returns true otherwise you may break the implementation, which is using a semaphore. */
   public boolean acquireLock() {
      try {
         dataLock.acquire();
         return true;
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt(); // forward the interrupt to fail elsewhere as well in case it was issued
         return false;
      }
   }

   public void releaseLock() {
      dataLock.release();
   }

   private void flushData(List<DBData> dbData, StorageTX storageTX) {
      if (!acquireLock()) {
         return;
      }
      try {
         dbData.forEach(DBData::lineUp);
         pendingData.addAll(dbData);
         pendingData.add(new TXDone((DatabaseStoreTX) storageTX));
      } finally {
         releaseLock();
      }
      delay();
   }

   private void flushData(DBData dbData) {
      if (!acquireLock()) {
         return;
      }
      try {
         dbData.lineUp();
         pendingData.add(dbData);
      } finally {
         releaseLock();
      }
      delay();
   }

   private DatabaseStoreTX castTX(StorageTX storageTX) {
      return (DatabaseStoreTX) storageTX;
   }

   public void storeMessage(StorageTX storageTX,
                            long messageID,
                            Supplier<ActiveMQBuffer> messageBufferSupplier,
                            Long tx,
                            int memoryEstimate,
                            IOCompletion callback) {
      castTX(storageTX).addData(new MessageData(messageID, messageBufferSupplier, tx, memoryEstimate, callback));
   }

   public void storeMessage(long messageID,
                            Supplier<ActiveMQBuffer> messageBufferSupplier,
                            Long tx,
                            int memoryEstimate,
                            IOCompletion callback) {
      flushData(new MessageData(messageID, messageBufferSupplier, tx, memoryEstimate, callback));
   }

   public void deleteMessage(long messageID, IOCompletion callback) {
      flushData(new DeleteMessageData(messageID, callback));
   }

   public void ackMessage(long queueID, long messageID, IOCompletion callback) {
      flushData(new DeleteReferenceData(queueID, messageID, callback));
   }

   public void ackMessage(StorageTX storageTX, long txID, long queueID, long messageID, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteReferenceData(queueID, messageID, callback));
   }

   public void storeReference(StorageTX storageTX,
                              long messageID,
                              long queueID,
                              boolean pendingDelivery,
                              Long txID,
                              IOCompletion callback) {
      castTX(storageTX).addData(new MessageReferenceData(messageID, queueID, pendingDelivery, txID, callback));
   }

   public void storeQueue(StorageTX storageTX,
                          long addressId,
                          long id,
                          String name,
                          String filter,
                          RoutingType routingType,
                          String queueConfigJson,
                          IOCompletion callback) {

      castTX(storageTX).addData(new QueueData(addressId, id, name, filter, routingType == RoutingType.MULTICAST, routingType == RoutingType.ANYCAST, queueConfigJson, callback));
   }

   public void updateQueue(StorageTX storageTX,
                           long addressId,
                           long id,
                           String name,
                           String filter,
                           RoutingType routingType,
                           String queueConfigJson,
                           IOCompletion callback) {
      castTX(storageTX).addData(new UpdateQueueData(addressId, id, name, filter, routingType == RoutingType.MULTICAST, routingType == RoutingType.ANYCAST, queueConfigJson, callback));
   }

   public void deleteQueue(StorageTX storageTX, long queueId, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteQueueData(queueId, callback));
   }

   public void deleteQueue(long queueId, IOCompletion callback) {
      flushData(new DeleteQueueData(queueId, callback));
   }

   public void storeReference(long messageID, long queueID, boolean pendingDelivery, Long txID, IOCompletion callback) {
      flushData(new MessageReferenceData(messageID, queueID, pendingDelivery, txID, callback));
   }

   public void deleteAddress(StorageTX storageTX, long addressId, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteAddressData(addressId, callback));
   }

   public void deleteAddress(long addressId, IOCompletion callback) {
      flushData(new DeleteAddressData(addressId, callback));
   }

   public void storeAddressInfo(StorageTX storageTX,
                                long id,
                                String address,
                                boolean isMulticast,
                                boolean isAnycast,
                                IOCompletion callback) {
      castTX(storageTX).addData(new AddressData(id, address, isMulticast, isAnycast, callback));
   }

   public void storePage(StorageTX storageTX,
                         long addressID,
                         long pageID,
                         long pageNR,
                         long messageID,
                         Supplier<ActiveMQBuffer> messageBufferSupplier,
                         Long txID,
                         IOCompletion callback) {
      castTX(storageTX).addData(new PageData(addressID, pageID, pageNR, messageID, messageBufferSupplier, txID, callback));
   }

   public void storePage(long addressID,
                         long pageID,
                         long pageNR,
                         long messageID,
                         Supplier<ActiveMQBuffer> messageBufferSupplier,
                         Long txID,
                         IOCompletion callback) {
      flushData(new PageData(addressID, pageID, pageNR, messageID, messageBufferSupplier, txID, callback));
   }

   public void deletePage(long addressID, long pageID, IOCompletion callback) {
      flushData(new DeletePageData(addressID, pageID, callback));
   }

   public void storePageRef(StorageTX storageTX,
                            long addressID,
                            long pageID,
                            long pageNR,
                            long queueID,
                            IOCompletion callback) {
      castTX(storageTX).addData(new PageRefData(addressID, pageID, pageNR, queueID, callback));
   }

   public void storePageRef(long addressID, long pageID, long pageNR, long queueID, IOCompletion callback) {
      flushData(new PageRefData(addressID, pageID, pageNR, queueID, callback));
   }

   public void deletePageRef(long addressID, long pageID, long pageNR, long queueID, IOCompletion callback) {
      flushData(new DeletePageRefData(addressID, pageID, pageNR, queueID, callback));
   }

   public void deletePageReferences(StorageTX storageTX, long addressID, long pageID, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteAllPageRefData(addressID, pageID, callback));
   }

   public void deletePageReferences(long addressID, long pageID, IOCompletion callback) {
      flushData(new DeleteAllPageRefData(addressID, pageID, callback));
   }

   public void storeGenericData(long id,
                                byte recordType,
                                Long txId,
                                Supplier<ActiveMQBuffer> dataSupplier,
                                IOCompletion callback) {
      flushData(new GenericData(id, recordType, txId, dataSupplier, callback));
   }

   public void storeGenericData(StorageTX storageTX,
                                long id,
                                byte recordType,
                                Long txId,
                                Supplier<ActiveMQBuffer> dataSupplier,
                                IOCompletion callback) {
      castTX(storageTX).addData(new GenericData(id, recordType, txId, dataSupplier, callback));
   }

   public void updateGenericData(long id, Long txId, Supplier<ActiveMQBuffer> dataSupplier, IOCompletion callback) {
      flushData(new UpdateGenericData(id, txId, dataSupplier, callback));
   }

   public void updateGenericData(StorageTX storageTX,
                                 long id,
                                 Long txId,
                                 Supplier<ActiveMQBuffer> dataSupplier,
                                 IOCompletion callback) {
      castTX(storageTX).addData(new UpdateGenericData(id, txId, dataSupplier, callback));
   }

   public void deleteGenericData(long id, IOCompletion callback) {
      flushData(new DeleteGenericData(id, callback));
   }

   public void deleteGenericData(StorageTX storageTX, long id, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteGenericData(id, callback));
   }

   public void storeBindingsGenericData(long id,
                                        byte recordType,
                                        Long txId,
                                        Supplier<ActiveMQBuffer> dataSupplier,
                                        IOCompletion callback) {
      flushData(new GenericData(id, recordType, txId, dataSupplier, true, callback));
   }

   public void storeBindingsGenericData(StorageTX storageTX,
                                        long id,
                                        byte recordType,
                                        Long txId,
                                        Supplier<ActiveMQBuffer> dataSupplier,
                                        IOCompletion callback) {
      castTX(storageTX).addData(new GenericData(id, recordType, txId, dataSupplier, true, callback));
   }

   public void updateBindingsGenericData(long id,
                                         Long txId,
                                         Supplier<ActiveMQBuffer> dataSupplier,
                                         IOCompletion callback) {
      flushData(new UpdateGenericData(id, txId, dataSupplier, true, callback));
   }

   public void updateBindingsGenericData(StorageTX storageTX,
                                         long id,
                                         Long txId,
                                         Supplier<ActiveMQBuffer> dataSupplier,
                                         IOCompletion callback) {
      castTX(storageTX).addData(new UpdateGenericData(id, txId, dataSupplier, true, callback));
   }

   public void deleteBindingsGenericData(long id, IOCompletion callback) {
      flushData(new DeleteGenericData(id, true, callback));
   }

   public void deleteBindingsGenericData(StorageTX storageTX, long id, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteGenericData(id, true, callback));
   }

   List<DBData> extractTaskList() {
      if (!acquireLock()) {
         return Collections.emptyList();
      }
      ArrayList<DBData> tasksToRun;
      try {
         if (pendingData.isEmpty()) {
            return null;
         }
         tasksToRun = new ArrayList<>(pendingData.size());
         Iterator<DBData> iter = pendingData.iterator();
         while (iter.hasNext()) {
            DBData data = iter.next();
            IOCompletion ctx = data.getContext();
            // You could have a worker already doing pending tasks on a given context.
            // If there's a worker already working on that context we need to wait it to finish before we do anything on this context
            // this is to avoid out of order events
            // also the context.workerDone is called while holding the lock on DataManager, to maintain consistency.
            if (ctx == null || !ctx.isWorking()) {
               tasksToRun.add(data);
               iter.remove();
            } else {
               if (logger.isDebugEnabled()) {
                  logger.debug("Keeping data {} for later as there's a worker on it still, current pendingTasks = {}", data, data.getContext().getActiveWorkers());
               }
            }
         }
      } finally {
         releaseLock();
      }
      return tasksToRun;
   }

   @Override
   public void run() {
      try {
         flush();
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   public void workerDone(DataWorker worker) {
      this.workers.offer(worker);
      if (acquireLock()) {
         try {
            // I need to check if pendingData is empty and schedule a delay as this worker is done
            // because you could have had previous tasks that couldn't be sent as the operationContext was
            // busy in some other worker.
            if (!pendingData.isEmpty()) {
               delay();
            }
         } finally {
            releaseLock();
         }
      }
   }

   public void flush() {
      DataWorker worker = workers.poll();
      if (worker == null) {
         demandBorrowedWorkersBack();
         this.delay();
         return;
      }

      List<DBData> dataList = extractTaskList();
      if (dataList != null && !dataList.isEmpty()) {
         logger.info("Extracted dataList with {} elements", dataList.size());
         // At this point we don't need a lock. we already have the list of tasks, and workUp is called from a single thread
         ioCompletions(dataList).forEach(IOCompletion::workUp);
         worker.setTaskList(dataList);
         executorService.execute(worker);
      } else {
         workerDone(worker);
      }

      BaseInterceptor interceptor;
      while ((interceptor = scheduledQueries.poll()) != null) {
         if (!dispatchInterceptor(interceptor)) {
            return;
         }
      }
   }

   /** *
    * Executes a query using the current thread.
    * Use this method sparingly. it's good for startup conditions.
    * @param consumer the worker that will perform the query
    * @param afterCommit
    * @param connectionPoolTimeout
    * @param unit
    */
   public void executeQuery(SQLConsumer<DataWorker> consumer, Runnable afterCommit, int connectionPoolTimeout, TimeUnit unit) throws Exception {
      try {
         DataWorker worker = workers.poll(connectionPoolTimeout, unit);
         if (worker == null) {
            throw ActiveMQDirectDBBundle.BUNDLE.timedOutWaitingForWorker(connectionPoolTimeout, unit.toString());
         }
         QueryInterceptor queryInterceptor = new QueryInterceptor(null, consumer, afterCommit);
         queryInterceptor.run();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e.getMessage(), e);
      }
   }


   public void executeQuery(Executor targetExecutor, SQLConsumer<DataWorker> consumer, Runnable afterCommit) {
      dispatchInterceptor(new QueryInterceptor(targetExecutor, consumer, afterCommit));
   }

   private boolean dispatchInterceptor(BaseInterceptor interceptor) {
      DataWorker worker = workers.poll();
      if (worker != null) {
         interceptor.setWorker(worker);
         interceptor.getExecutor().execute(interceptor);
         return true;
      } else {
         demandBorrowedWorkersBack();
         scheduledQueries.offer(interceptor);
         delay();
         return false;
      }
   }

   /**
    * Borrow a worker from the pool for extended use (e.g. keeping a ResultSet cursor open
    * across multiple batch reads). The worker is held on a lease that expires after
    * {@code leaseTimeoutMillis} of inactivity. Each time useful work is done, the caller
    * should call {@link BorrowedWorker#renewLease()} to reset the timeout.
    *
    * <p>If the pool is exhausted, this method signals all currently borrowed workers
    * via {@link BorrowedWorker#needConnectionBack} and queues the borrow request
    * for dispatch when a worker becomes available.</p>
    *
    * @param targetExecutor         the executor the borrowed worker is bound to (thread affinity)
    * @param leaseTimeoutMillis     how long the worker can be idle before the lease expires
    * @param cleanupAction          optional action to run before returning the worker to the pool
    * @param borrowedWorkerConsumer called with the BorrowedWorker once a worker is available
    */
   public void borrowWorker(Executor targetExecutor,
                             long leaseTimeoutMillis,
                             Runnable cleanupAction,
                             Runnable resumeAction,
                             Consumer<BorrowedWorker> borrowedWorkerConsumer) {
      borrowWorker(targetExecutor, leaseTimeoutMillis, cleanupAction, resumeAction, borrowedWorkerConsumer, null);
   }

   public void borrowWorker(Executor targetExecutor,
                             long leaseTimeoutMillis,
                             Runnable cleanupAction,
                             Runnable resumeAction,
                             Consumer<BorrowedWorker> borrowedWorkerConsumer,
                             String description) {
      BorrowInterceptor interceptor = new BorrowInterceptor(targetExecutor, leaseTimeoutMillis, cleanupAction, resumeAction, borrowedWorkerConsumer, description);
      DataWorker worker = workers.poll();
      if (worker != null) {
         interceptor.setWorker(worker);
         interceptor.run();
      } else {
         demandBorrowedWorkersBack();
         scheduledQueries.offer(interceptor);
         delay();
      }
   }

   /**
    * Called by {@link BorrowedWorker} when it is returned (either explicitly, on lease expiry,
    * or on demand). Returns the underlying worker to the pool.
    */
   void returnBorrowedWorker(BorrowedWorker borrowedWorker) {
      borrowedWorkers.remove(borrowedWorker);
      workerDone(borrowedWorker.getWorker());
   }

   /**
    * Signal all borrowed workers that the pool needs connections back,
    * but only if the number of borrowed connections exceeds {@link #maxReadConnections}.
    * A maxReadConnections of 0 means no cap — demand back whenever the pool is empty.
    */
   private void demandBorrowedWorkersBack() {
      if (borrowedWorkers.isEmpty()) {
         return;
      }
      int borrowedCount = borrowedWorkers.size();
      if (maxReadConnections > 0 && borrowedCount <= maxReadConnections) {
         return;
      }
      logReturningWorkers(borrowedCount);
      for (BorrowedWorker borrowed : borrowedWorkers) {
         borrowed.demandBack();
      }
   }

   private void logReturningWorkers(int borrowedCount) {
      TableOut table = new TableOut(" ", 0, new int[]{60});
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      table.printTopSeparator(ps);
      for (BorrowedWorker borrowed : borrowedWorkers) {
         table.print(ps, new String[]{borrowed.toString()});
      }
      table.printBottomSeparator(ps);
      ActiveMQDatabaseLogger.LOGGER.demandingBorrowedConnectionsBack(borrowedCount, maxReadConnections, baos.toString());
   }

   private static abstract class BaseInterceptor implements Runnable {
      final SQLConsumer<DataWorker> consumer;
      final Executor executor;
      final Runnable afterCommit;
      DataWorker worker;

      BaseInterceptor(Executor executor, SQLConsumer<DataWorker> consumer, Runnable afterCommit) {
         this.consumer = consumer;
         this.executor = executor;
         this.afterCommit = afterCommit;
      }

      public void setWorker(DataWorker worker) {
         this.worker = worker;
      }

      public Executor getExecutor() {
         return executor;
      }
   }

   private class BorrowInterceptor extends BaseInterceptor {
      final long leaseTimeoutMillis;
      final Runnable cleanupAction;
      final Runnable resumeAction;
      final Consumer<BorrowedWorker> borrowedWorkerConsumer;
      final String description;

      BorrowInterceptor(Executor executor,
                        long leaseTimeoutMillis,
                        Runnable cleanupAction,
                        Runnable resumeAction,
                        Consumer<BorrowedWorker> borrowedWorkerConsumer,
                        String description) {
         super(executor, null, null);
         this.leaseTimeoutMillis = leaseTimeoutMillis;
         this.cleanupAction = cleanupAction;
         this.resumeAction = resumeAction;
         this.borrowedWorkerConsumer = borrowedWorkerConsumer;
         this.description = description;
      }

      @Override
      public void run() {
         BorrowedWorker borrowed = new BorrowedWorker(DataManager.this, this.worker, executor, scheduledExecutorService, leaseTimeoutMillis, cleanupAction, resumeAction, description);
         borrowedWorkers.add(borrowed);
         borrowedWorkerConsumer.accept(borrowed);
      }
   }

   private class QueryInterceptor extends BaseInterceptor {

      QueryInterceptor(Executor executor, SQLConsumer<DataWorker> consumer, Runnable afterCommit) {
         super(executor, consumer, afterCommit);
      }

      @Override
      public void run() {
         try {
            SQLException retryException = worker.executeWithRetry(consumer);
            if (retryException != null) {
               criticalError(retryException);
               return;
            }
            if (afterCommit != null) {
               try {
                  worker.commit();
               } catch (SQLException e) {
                  criticalError(e);
                  return;
               }
               afterCommit.run();
            }
         } finally {
            workerDone(worker);
         }
      }
   }

}
