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

package org.apache.activemq.artemis.core.settings.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.utils.SizeAwareMetric;

// This is the resource usage of a single Address, to be aggregated at the Address Level, within PagingStore / AddressSizeLimiter
public class ResourceRoot {

   private final SizeAwareMetric sizeMetric = new SizeAwareMetric();
   // notice there's always only 1 address at the root level. it will be used to aggregate upper levels
   private final SizeAwareMetric addressMetric = new SizeAwareMetric();
   private final SizeAwareMetric queuesMetric = new SizeAwareMetric();

   private record Callbacks (SizeAwareMetric.AggregateCallback sizeCallback, SizeAwareMetric.AggregateCallback addressCallback, SizeAwareMetric.AggregateCallback queueCallback) {}

   private Map<ResourceQuota, Callbacks> callbacksMap = new HashMap<>();

   public void addParent(ResourceQuota parent) {
      if (callbacksMap.containsKey(parent)) {
         return;
      }
      SizeAwareMetric.AggregateCallback size = parent::aggregateSize;
      SizeAwareMetric.AggregateCallback address = parent::aggregateAddress;
      SizeAwareMetric.AggregateCallback queue = parent::aggregateQueue;

      Callbacks callbacks = new Callbacks(size, address, queue);
      sizeMetric.addCallback(size);
      addressMetric.addCallback(address);
      queuesMetric.addCallback(queue);

      callbacksMap.put(parent, callbacks);
   }


   public long addSize(long size) {
      return sizeMetric.addSize(size);
   }

   public void incrementQueue() {
      queuesMetric.incrementElement();
   }

   public void decrementQueue() {
      queuesMetric.decrementElement();
   }

   public void removeParent(ResourceQuota parent) {
      Callbacks callbacks = callbacksMap.remove(parent);
      if (callbacks != null) {
         sizeMetric.removeCallback(callbacks.sizeCallback);
         addressMetric.removeCallback(callbacks.addressCallback);
         queuesMetric.removeCallback(callbacks.queueCallback);
      }
   }


}
