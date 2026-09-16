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

import org.apache.activemq.artemis.core.server.StorageMessageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseStorageMessageReader implements StorageMessageReader {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final QueueImpl queue;

   public DatabaseStorageMessageReader(QueueImpl queue) {
      this.queue = queue;
   }

   @Override
   public void scheduleRead(boolean scheduleExpiry) {
      logger.info("Scheduling read...", new Exception());
   }

   @Override
   public void checkRead() {
      new Exception("checkRead").printStackTrace();
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
