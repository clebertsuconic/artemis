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

import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.artemis.database.worker.DataWorker;

public class MessageData extends DBData {

   public final long messageID;
   public final Supplier<ActiveMQBuffer> messageBufferSupplier;
   public final int memoryEstimate;
   public final boolean isLarge;
   public final boolean isCore;
   public final Supplier<ActiveMQBuffer> largeBodySupplier;

   public MessageData(long messageID,
                      Supplier<ActiveMQBuffer> messageBufferSupplier,
                      int memoryEstimate,
                      boolean isLarge,
                      boolean isCore,
                      Supplier<ActiveMQBuffer> largeBodySupplier,
                      IOCompletion context) {
      super(context);
      this.messageID = messageID;
      this.messageBufferSupplier = messageBufferSupplier;
      this.memoryEstimate = memoryEstimate;
      this.isLarge = isLarge;
      this.isCore = isCore;
      this.largeBodySupplier = largeBodySupplier;
   }

   @Override
   public void perform(DataWorker worker) {
      worker.insertMessageStatement.addElement(this, context);
   }

   @Override
   public String toString() {
      return "MessageData{" + "messageID=" + messageID + ", tx=" + tx + ", memoryEstimate=" + memoryEstimate + '}';
   }
}
