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

package org.apache.artemis.database.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;

/**
 * Carries a single message row merged with all of its queue references.
 * Produced by {@code MessagesAndReferencesDBQuery}, which collapses the
 * JOIN result set so that each consumer invocation represents exactly one
 * message together with every queue it is routed to.
 */
public class MessageDataReferenceMerged {

   /** Per-queue entry held inside a merged message row. */
   public static class QueueRef {
      public final long queueID;
      public final boolean paged;

      public QueueRef(long queueID, boolean paged) {
         this.queueID = queueID;
         this.paged = paged;
      }

      @Override
      public String toString() {
         return paged ? queueID + "*" : String.valueOf(queueID);
      }
   }

   public final long messageID;
   public final Long tx;
   public final int memoryEstimate;
   public final Supplier<ActiveMQBuffer> messageBufferSupplier;
   public final List<QueueRef> queues;

   public MessageDataReferenceMerged(long messageID, Long tx, int memoryEstimate,
                                     Supplier<ActiveMQBuffer> messageBufferSupplier,
                                     List<QueueRef> queues) {
      this.messageID = messageID;
      this.tx = tx;
      this.memoryEstimate = memoryEstimate;
      this.messageBufferSupplier = messageBufferSupplier;
      this.queues = Collections.unmodifiableList(new ArrayList<>(queues));
   }
}
