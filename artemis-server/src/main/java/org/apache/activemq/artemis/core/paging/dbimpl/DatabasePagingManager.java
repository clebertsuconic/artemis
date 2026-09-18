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
package org.apache.activemq.artemis.core.paging.dbimpl;

import java.util.Map;
import java.util.concurrent.Future;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.impl.AbstracPagingManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

public class DatabasePagingManager extends AbstracPagingManager {

   public DatabasePagingManager(final PagingStoreFactory pagingStoreFactory,
                                final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                final long maxSize,
                                final long maxMessages,
                                final SimpleString managementAddress,
                                final ActiveMQServer server) {
      super(pagingStoreFactory, addressSettingsRepository, maxSize, maxMessages, managementAddress, server);
   }

   @Override
   public boolean requireRebuildCounters() {
      return false;
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void addTransaction(PageTransactionInfo pageTransaction) {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public PageTransactionInfo getTransaction(long transactionID) {
      return null;
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void removeTransaction(long transactionID) {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public Map<Long, PageTransactionInfo> getTransactions() {
      return Map.of();
   }

   @Override
   public void reloadStores() throws Exception {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void deletePageStore(SimpleString storeName) throws Exception {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void processReload() throws Exception {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void disableCleanup() {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void resumeCleanup() {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public boolean isDiskFull() {
      return false;
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public long getDiskUsableSpace() {
      return 0;
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public long getDiskTotalSpace() {
      return 0;
   }

   // TODO: Consider removing from here and interface, and using a specific cast where needed
   @Override
   public void counterSnapshot() {
   }


   // TODO: Is there a way to have start / stop moved from PagingManagerImpl into Abstract and be reused? maybe there are specific files things that need to be specialized
   @Override
   public void start() throws Exception {
   }

   @Override
   public void stop() throws Exception {
   }
}
