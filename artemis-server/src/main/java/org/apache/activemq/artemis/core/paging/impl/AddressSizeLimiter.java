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

import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.SizeAwareMetric;

public abstract class AddressSizeLimiter {

   protected long maxSize;

   protected int maxPageReadBytes = -1;

   protected int maxPageReadMessages = -1;

   protected int prefetchPageBytes = -1;

   protected int prefetchPageMessages = -1;

   protected long maxMessages;

   // Bytes consumed by the queue on the memory
   protected final SizeAwareMetric size;

   public AddressSizeLimiter() {
      this.size = new SizeAwareMetric();
   }

   protected void configureSizeMetric() {
      size.setMax(maxSize, maxSize, maxMessages, maxMessages);
   }

   protected void applySetting(final AddressSettings addressSettings, final boolean firstTime) {
      maxSize = addressSettings.getMaxSizeBytes();

      maxPageReadMessages = addressSettings.getMaxReadPageMessages();

      prefetchPageMessages = addressSettings.getPrefetchPageMessages();

      maxPageReadBytes = addressSettings.getMaxReadPageBytes();

      prefetchPageBytes = addressSettings.getPrefetchPageBytes();

      maxMessages = addressSettings.getMaxSizeMessages();

      configureSizeMetric();
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


}
