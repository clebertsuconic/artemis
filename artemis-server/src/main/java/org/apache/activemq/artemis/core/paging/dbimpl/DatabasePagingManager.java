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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.impl.AddressSizeLimiter;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

public class DatabasePagingManager implements PagingManager {



   @Override
   public boolean requireRebuildCounters() {
      return false;
   }

   @Override
   public PagingStore getPageStore(SimpleString address) throws Exception {
      return null;
   }

   @Override
   public void addTransaction(PageTransactionInfo pageTransaction) {

   }

   @Override
   public PageTransactionInfo getTransaction(long transactionID) {
      return null;
   }

   @Override
   public void removeTransaction(long transactionID) {

   }

   @Override
   public Map<Long, PageTransactionInfo> getTransactions() {
      return Map.of();
   }

   @Override
   public void reloadStores() throws Exception {

   }

   @Override
   public SimpleString[] getStoreNames() {
      return new SimpleString[0];
   }

   @Override
   public void deletePageStore(SimpleString storeName) throws Exception {

   }

   @Override
   public void processReload() throws Exception {

   }

   @Override
   public void disableCleanup() {

   }

   @Override
   public void resumeCleanup() {

   }

   @Override
   public void addBlockedStore(AddressSizeLimiter store) {

   }

   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {

   }

   @Override
   public void lock() {

   }

   @Override
   public void unlock() {

   }

   @Override
   public PagingManager addSize(int size, boolean sizeOnly) {
      return null;
   }

   @Override
   public boolean isUsingGlobalSize() {
      return false;
   }

   @Override
   public boolean isGlobalFull() {
      return false;
   }

   @Override
   public boolean isDiskFull() {
      return false;
   }

   @Override
   public long getDiskUsableSpace() {
      return 0;
   }

   @Override
   public long getDiskTotalSpace() {
      return 0;
   }

   @Override
   public void checkMemory(Runnable runWhenAvailable) {

   }

   @Override
   public void counterSnapshot() {

   }

   @Override
   public void start() throws Exception {

   }

   @Override
   public void stop() throws Exception {

   }

   @Override
   public boolean isStarted() {
      return false;
   }

   @Override
   public void onChange() {

   }
}
