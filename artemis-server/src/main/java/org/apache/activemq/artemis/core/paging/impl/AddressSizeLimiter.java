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
package org.apache.activemq.artemis.core.paging.impl;

import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.DiskFullMessagePolicy;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.apache.activemq.artemis.utils.FutureLatch;
import org.apache.activemq.artemis.utils.SizeAwareMetric;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AddressSizeLimiter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private String address;

   protected long maxSize;

   protected int maxPageReadBytes = -1;

   protected int maxPageReadMessages = -1;

   protected int prefetchPageBytes = -1;

   protected int prefetchPageMessages = -1;

   protected long maxMessages;

   private volatile boolean full;

   private volatile boolean blocking = false;

   private volatile boolean blockedViaManagement = false;

   private volatile AddressFullMessagePolicy addressFullMessagePolicy;

   private DiskFullMessagePolicy diskFullMessagePolicy;

   // This lock mostly protects the paging field. It is also used to block producers in eventual cases such as dropping
   // a queue, but mostly to protect if the storage is in paging mode.
   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   protected volatile boolean paging = false;

   private long rejectThreshold;

   private final StorageManager storageManager;

   private final ArtemisExecutor executor;

   protected volatile boolean running = false;

   public boolean isStarted() {
      return running;
   }

   // Internal components such as mirroring could enforce a different page full message policy
   // differing from the AddressSettings
   // Example: User configured sync mirroring while default address-settings is PAGE. We must use Block on that case
   //          User configured non sync mirroring while configured drop. We must use page. (always paged)
   private volatile AddressFullMessagePolicy enforcedAddressFullMessagePolicy;


   // Bytes consumed by the queue on the memory
   private final SizeAwareMetric size;

   private final PagingManager pagingManager;

   private void overSized() {
      full = true;
   }

   public void block() {
      if (!blockedViaManagement) {
         ActiveMQServerLogger.LOGGER.blockingViaControl(address);
      }
      blockedViaManagement = true;
   }

   public void unblock() {
      if (blockedViaManagement) {
         ActiveMQServerLogger.LOGGER.unblockingViaControl(address);
      }
      blockedViaManagement = false;
      checkReleasedMemory();
   }


   private void underSized() {
      full = false;
      checkReleasedMemory();
   }

   public boolean isBlockedViaManagement() {
      return blockedViaManagement;
   }


   // To be used on isDropMessagesWhenFull
   public boolean isFull() {
      return full || pagingManager.isGlobalFull();
   }


   public boolean isStorePaging() {
      return paging;
   }

   public boolean isPaging() {
      AddressFullMessagePolicy policy = this.addressFullMessagePolicy;
      if (policy == AddressFullMessagePolicy.BLOCK) {
         return false;
      }
      if (policy == AddressFullMessagePolicy.FAIL) {
         return isFull();
      }
      if (policy == AddressFullMessagePolicy.DROP) {
         return isFull();
      }
      return paging;
   }

   public int getAddressLimitPercent() {
      final long currentUsage = getAddressSize();
      if (maxSize > 0) {
         return (int) (currentUsage * 100 / maxSize);
      } else if (pagingManager.isUsingGlobalSize()) {
         return (int) (currentUsage * 100 / pagingManager.getMaxSize());
      }
      return 0;
   }

   public boolean isRejectingMessages() {
      if (addressFullMessagePolicy != AddressFullMessagePolicy.BLOCK) {
         return false;
      }
      return rejectThreshold != AddressSettings.DEFAULT_ADDRESS_REJECT_THRESHOLD && getAddressSize() > rejectThreshold;
   }

   public AddressSizeLimiter(StorageManager storageManager, PagingManager pagingManager, String address, ArtemisExecutor executor) {
      this.executor = executor;
      this.address = address;
      this.pagingManager = pagingManager;
      this.storageManager = storageManager;
      this.size = new SizeAwareMetric().setUnderCallback(this::underSized).setOverCallback(this::overSized).
         setOnSizeCallback(pagingManager::addSize);

   }

   public boolean checkReleasedMemory() {
      if (!blockedViaManagement && !pagingManager.isGlobalFull() && !full) {
         executor.execute(this::memoryReleased);
         if (blocking) {
            ActiveMQServerLogger.LOGGER.unblockingMessageProduction(address, getInfo());
            blocking = false;
            return true;
         }
      }

      return !blocking;
   }

   private final Queue<Runnable> onMemoryFreedRunnables = new ConcurrentLinkedQueue<>();

   private void memoryReleased() {
      Runnable runnable;

      while ((runnable = onMemoryFreedRunnables.poll()) != null) {
         runnable.run();
      }
   }

   public AddressFullMessagePolicy getAddressFullMessagePolicy() {
      return addressFullMessagePolicy;
   }

   public AddressSizeLimiter enforceAddressFullMessagePolicy(AddressFullMessagePolicy enforcedAddressFullMessagePolicy) {
      this.addressFullMessagePolicy = enforcedAddressFullMessagePolicy;
      this.enforcedAddressFullMessagePolicy = enforcedAddressFullMessagePolicy;
      return this;
   }

   protected void configureSizeMetric() {
      size.setMax(maxSize, maxSize, maxMessages, maxMessages);
   }

   public PagingManager getPagingManager() {
      return pagingManager;
   }

   protected void applySetting(final AddressSettings addressSettings, final boolean firstTime) {
      maxSize = addressSettings.getMaxSizeBytes();

      maxPageReadMessages = addressSettings.getMaxReadPageMessages();

      prefetchPageMessages = addressSettings.getPrefetchPageMessages();

      maxPageReadBytes = addressSettings.getMaxReadPageBytes();

      prefetchPageBytes = addressSettings.getPrefetchPageBytes();

      maxMessages = addressSettings.getMaxSizeMessages();

      configureSizeMetric();


      if (enforcedAddressFullMessagePolicy != null) {
         this.addressFullMessagePolicy = enforcedAddressFullMessagePolicy;
      } else {
         addressFullMessagePolicy = addressSettings.getAddressFullMessagePolicy();
      }

      diskFullMessagePolicy = addressSettings.getDiskFullMessagePolicy();

      rejectThreshold = addressSettings.getMaxSizeBytesRejectThreshold();
   }


   public long getAddressSize() {
      return size.getSize();
   }

   public long getAddressElements() {
      return size.getElements();
   }

   public long getMaxSize() {
      return maxSize;
   }

   public int getMaxPageReadBytes() {
      return maxPageReadBytes;
   }

   public int getPrefetchPageBytes() {
      return prefetchPageBytes;
   }

   public int getMaxPageReadMessages() {
      return maxPageReadMessages;
   }

   public int getPrefetchPageMessages() {
      return prefetchPageMessages;
   }

   public boolean startPaging() {
      if (!running) {
         return false;
      }

      readLock();
      try {
         // I'm not calling isPaging() here because i need to be atomic and hold a lock.
         if (paging) {
            return false;
         }
      } finally {
         readUnlock();
      }

      new Exception("Trace paging").printStackTrace(System.out);

      // We need to guarantee a readLock on the storageManager before starting paging. This is because the replication
      // manager will get a list of files to synchronize while holding a writeLock on the storageManager. So we must
      // guarantee a readLock here otherwise the list might be wrong.
      try (ArtemisCloseable readLock = storageManager.closeableReadLock()) {
         // if the first check failed, we do it again under a global currentPageLock
         // (writeLock) this time
         writeLock();
         try {
            // Same notes from previous if (paging) on this method will apply here
            if (paging) {
               return false;
            }
            beginPage();

            paging = true;
            ActiveMQServerLogger.LOGGER.pageStoreStart(address, getInfo());

            return true;
         } finally {
            writeUnlock();
         }
      }
   }

   protected abstract boolean beginPage();


   public long addSize(final int size, boolean sizeOnly, boolean affectGlobal) {
      long newSize = this.size.addSize(size, sizeOnly, affectGlobal);

      boolean globalFull = pagingManager.isGlobalFull();

      if (newSize < 0) {
         ActiveMQServerLogger.LOGGER.negativeAddressSize(address.toString(), newSize);
      }

      if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK || addressFullMessagePolicy == AddressFullMessagePolicy.FAIL) {
         if (pagingManager.isUsingGlobalSize() && !globalFull || maxSize != -1) {
            checkReleasedMemory();
         }
      } else if (addressFullMessagePolicy == AddressFullMessagePolicy.PAGE) {
         if (size > 0) {
            if (globalFull || full) {
               startPaging();
            }
         }
      }

      return newSize;
   }

   public void readLock() {
      readLock(-1L);
   }

   public boolean readLock(long timeout) {
      try {
         if (timeout == -1) {
            while (true) {
               if (tryReadLock(1, TimeUnit.SECONDS)) {
                  return true;
               }
            }
         } else {
            return tryReadLock(timeout, TimeUnit.MILLISECONDS);
         }
      } catch (InterruptedException e) {
         logger.warn(e.getMessage(), e);
         Thread.currentThread().interrupt();
         return false;
      }
   }

   private boolean tryReadLock(long timeout, TimeUnit unit) throws InterruptedException {
      if (lock.readLock().tryLock(timeout, unit)) {
         return true;
      } else {
         if (logger.isTraceEnabled()) {
            logger.trace("Not able to read lock");
         }
         return false;
      }
   }

   public void readUnlock() {
      lock.readLock().unlock();
   }

   public void writeLock() {
      writeLock(-1L);
   }

   public boolean writeLock(long timeout) {
      try {
         if (timeout == -1) {
            while (true) {
               if (tryWriteLock(1, TimeUnit.SECONDS)) {
                  return true;
               }
            }
         } else {
            return tryWriteLock(timeout, TimeUnit.MILLISECONDS);
         }
      } catch (InterruptedException e) {
         logger.warn(e.getMessage(), e);
         Thread.currentThread().interrupt();
         return false;
      }
   }

   private boolean tryWriteLock(long timeout, TimeUnit unit) throws InterruptedException {
      if (lock.writeLock().tryLock(timeout, unit)) {
         return true;
      } else {
         if (logger.isTraceEnabled()) {
            logger.trace("Not able to write lock");
         }
         return false;
      }

   }

   public void writeUnlock() {
      lock.writeLock().unlock();
   }

   public boolean checkMemory(final Runnable runWhenAvailable, Consumer<AtomicRunnable> blockedCallback) {
      return checkMemory(true, runWhenAvailable, null, blockedCallback);
   }

   private void addToBlockList(AtomicRunnable atomicRunnable, Consumer<AtomicRunnable> accepted) {
      atomicRunnable.setCancel(onMemoryFreedRunnables::remove);
      onMemoryFreedRunnables.add(atomicRunnable);
      if (accepted != null) {
         accepted.accept(atomicRunnable);
      }
   }

   public boolean checkMemory(boolean runOnFailure, Runnable runWhenAvailableParameter, Runnable runWhenBlocking, Consumer<AtomicRunnable> blockedCallback) {
      AtomicRunnable runWhenAvailable = AtomicRunnable.checkAtomic(runWhenAvailableParameter);

      if (blockedViaManagement) {
         if (runWhenAvailable != null) {
            addToBlockList(runWhenAvailable, blockedCallback);
         }
         return false;
      }

      if (pagingManager.isDiskFull()) {
         if (diskFullMessagePolicy == DiskFullMessagePolicy.FAIL) {
            if (runOnFailure) {
               addToBlockList(runWhenAvailable, blockedCallback);
               pagingManager.addBlockedStore(this);
            }
            return false;
         }

         if (diskFullMessagePolicy == null || diskFullMessagePolicy == DiskFullMessagePolicy.BLOCK) {
            if (runWhenBlocking != null) {
               runWhenBlocking.run();
            }

            addToBlockList(runWhenAvailable, blockedCallback);

            // Avoid a race condition see description below
            if (!pagingManager.isDiskFull()) {
               runWhenAvailable.run();
               onMemoryFreedRunnables.remove(runWhenAvailable);
            } else {
               pagingManager.addBlockedStore(this);

               if (!blocking) {
                  ActiveMQServerLogger.LOGGER.blockingDiskFull(address);
                  blocking = true;
               }
            }

            return true;
         }
      } else {
         if (addressFullMessagePolicy == AddressFullMessagePolicy.FAIL && (maxSize != -1 || maxMessages != -1 || pagingManager.isUsingGlobalSize())) {
            if (isFull()) {
               if (runOnFailure && runWhenAvailable != null) {
                  addToBlockList(runWhenAvailable, blockedCallback);
                  pagingManager.addBlockedStore(this);
               }
               return false;
            }
         } else if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK && (maxMessages != -1 || maxSize != -1 || pagingManager.isUsingGlobalSize())) {
            if (this.full || pagingManager.isGlobalFull()) {
               if (runWhenBlocking != null) {
                  runWhenBlocking.run();
               }

               addToBlockList(runWhenAvailable, blockedCallback);

               // We check again to avoid a race condition where the size can come down just after the element
               // has been added, but the check to execute was done before the element was added
               // NOTE! We do not fix this race by locking the whole thing, doing this check provides
               // MUCH better performance in a highly concurrent environment
               if (!pagingManager.isGlobalFull() && !full) {
                  // run it now
                  runWhenAvailable.run();
                  onMemoryFreedRunnables.remove(runWhenAvailable);
               } else {
                  if (pagingManager.isUsingGlobalSize()) {
                     pagingManager.addBlockedStore(this);
                  }

                  if (!blocking) {
                     ActiveMQServerLogger.LOGGER.blockingMessageProduction(address, getInfo());
                     blocking = true;
                  }
               }

               return true;
            }
         }
      }

      if (runWhenAvailable != null) {
         runWhenAvailable.run();
      }

      return true;
   }


   protected String getInfo() {
      return "AddressSizeLimiter::" + this.address;
   }


   public ArtemisExecutor getExecutor() {
      return executor;
   }

   public void execute(Runnable run) {
      executor.execute(run);
   }

   public void flushExecutors() {
      FutureLatch future = new FutureLatch();

      try {
         executor.execute(future);

         if (!future.await(60000)) {
            ActiveMQServerLogger.LOGGER.pageStoreTimeout(address);
         }
      } catch (Exception ignored) {
      }
   }

}
