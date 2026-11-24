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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.activemq.artemis.utils.SizeAwareMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Runtime resource quota tracker for hierarchical resource management.
 * <p>
 * This class tracks live quota usage (counters) and enforces limits defined in {@link ResourceQuotaConfig}.
 * ResourceQuota instances are NOT serializable - they are always rebuilt on broker restart by:
 * <ol>
 *   <li>Creating from ResourceQuotaConfig via {@link ResourceQuotaConfig#createRuntimeQuota()}</li>
 *   <li>Scanning existing addresses/queues to rebuild counters (during journal replay)</li>
 * </ol>
 * <p>
 * Three types of limits are enforced:
 * <ul>
 *   <li>max-message-bytes: Total bytes for messages across all addresses in this quota</li>
 *   <li>max-addresses: Maximum number of addresses in this quota</li>
 *   <li>max-queues: Maximum number of queues in this quota</li>
 * </ul>
 * <p>
 * Quotas can be organized in a parent-child hierarchy where child quotas count toward parent limits.
 * Quotas support wildcard templates via {@link ResourceQuotaConfig} that create runtime instances
 * on-demand when addresses match patterns.
 *
 * @see ResourceQuotaConfig for the configuration/limits definition
 */
public class ResourceQuota {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final long DEFAULT_MAX_MESSAGE_BYTES = -1;
   public static final int DEFAULT_MAX_ADDRESSES = -1;
   public static final int DEFAULT_MAX_QUEUES = -1;

   // Configuration (limits) - set from ResourceQuotaConfig
   // volatile ensures changes during reload are visible to all threads
   private final String name;
   private volatile Long maxMessageBytes;
   private volatile Integer maxAddresses;
   private volatile Integer maxQueues;

   // Runtime state (counters, relationships)
   // volatile for parent since it can be updated during reload/hierarchy changes
   private volatile ResourceQuota parent;
   private volatile boolean overSize = false;
   private volatile boolean overAddress = false;
   private volatile boolean overQueue = false;

   private final SizeAwareMetric sizeMetric;
   private final SizeAwareMetric addressCount;
   private final SizeAwareMetric queueCount;

   // ========================================================================
   // Constructor and Initialization
   // ========================================================================

   /**
    * Create a runtime quota instance. Typically called via {@link ResourceQuotaConfig#createRuntimeQuota()}.
    * Counters start at zero and are rebuilt by scanning existing addresses/queues.
    *
    * @param name the quota name
    */
   public ResourceQuota(String name) {
      this.name = name;
      this.maxMessageBytes = null;
      this.maxAddresses = null;
      this.maxQueues = null;
      this.sizeMetric = new SizeAwareMetric().setOverCallback(() -> overSize = true).setUnderCallback(() -> overSize = false);
      this.addressCount = new SizeAwareMetric().setOverCallback(() -> overAddress = true).setUnderCallback(() -> overAddress = false);
      this.queueCount = new SizeAwareMetric().setOverCallback(() -> overQueue = true).setUnderCallback(() -> overQueue = false);
   }


   // ========================================================================
   // Configuration Getters and Setters
   // ========================================================================

   public String getName() {
      return name;
   }

   public long getMaxMessageBytes() {
      return maxMessageBytes != null ? maxMessageBytes : DEFAULT_MAX_MESSAGE_BYTES;
   }

   public ResourceQuota setMaxMessageBytes(Long maxMessageBytes) {
      this.maxMessageBytes = maxMessageBytes;
      sizeMetric.setMax(maxMessageBytes, maxMessageBytes, -1, -1);
      return this;
   }

   public int getMaxAddresses() {
      return maxAddresses != null ? maxAddresses : DEFAULT_MAX_ADDRESSES;
   }

   public ResourceQuota setMaxAddresses(Integer maxAddresses) {
      this.maxAddresses = maxAddresses;
      int max = maxAddresses == null ? 0 : maxAddresses;
      addressCount.setMax(0, 0, max, max);
      return this;
   }

   public int getMaxQueues() {
      return maxQueues != null ? maxQueues : DEFAULT_MAX_QUEUES;
   }

   public ResourceQuota setMaxQueues(Integer maxQueues) {
      this.maxQueues = maxQueues;
      int max = maxQueues == null ? 0 : maxQueues;
      queueCount.setMax(0, 0, max, max);
      return this;
   }

   // ========================================================================
   // Byte Quota Operations
   // ========================================================================

   // This is for recursion adding
   public void aggregateSize(long deltaElements, long deltaSize) {
      sizeMetric.aggregate(deltaElements, deltaSize);

      if (logger.isDebugEnabled()) {
         logger.debug("Quota {} size changed by {}/{} to {}/{}", name, deltaElements, deltaSize, sizeMetric.getElements(), sizeMetric.getSize());
      }
   }

   // This is for recursion adding
   void aggregateQueue(long deltaElements, long deltaSize) {
      queueCount.aggregate(deltaElements, deltaSize);
   }

   // This is for recursion adding
   void aggregateAddress(long deltaElements, long deltaSize) {
      addressCount.aggregate(deltaElements, deltaSize);
   }

   /**
    * Get current size in bytes tracked by this quota.
    * Alias for getCurrentMessageBytes() for consistency with other getCurrentXXX methods.
    */
   public long getSize() {
      return getCurrentMessageBytes();
   }

   /**
    * Get current message bytes tracked by this quota.
    */
   public long getCurrentMessageBytes() {
      return sizeMetric.getSize();
   }

   // ========================================================================
   // Limit Checking Methods
   // ========================================================================

   /**
    * Check if adding bytes would exceed limits.
    * This is a non-modifying check - use addSize() after successfully routing the message.
    *
    * @param bytesToAdd the number of bytes to check
    * @return true if adding the bytes would stay within limits, false if adding would exceed
    */
   // @Gary remove bytesToAdd as a parameter. we don't how many bytes we will write until we get to PageStore. Extra complexity just to be strictly enforced.
   public boolean canAddBytes(long bytesToAdd) {
      if (overSize) {
         return false;
      }

      // @Gary: I need to remove the following. I don't know how many bytes are going to be written until I get to PagingStore::write
      // if (!sizeMetric.fitsBytes(bytesToAdd)) {
         // return false;
      // }

      if (parent != null) {
         return parent.canAddBytes(bytesToAdd);
      } else {
         return true;
      }
   }

   /**
    * Check if byte limit is exceeded.
    * Note: This is a reactive check. For proactive enforcement, use canAddBytes() before adding.
    *
    * @return true if current message bytes exceed maxMessageBytes
    */
   public boolean isByteLimitReached() {
      if (overSize) {
         return true;
      }

      if (parent != null) {
         return parent.isAddressLimitReached();
      } else {
         return false;
      }
   }

   /**
    * Check if address limit is reached or exceeded.
    *
    * @return true if current address count is at or above maxAddresses
    */
   public boolean isAddressLimitReached() {

      if (overAddress) {
         return true;
      }

      if (parent != null) {
         return parent.isAddressLimitReached();
      } else {
         return false;
      }
   }

   /**
    * Check if queue limit is reached or exceeded.
    *
    * @return true if current queue count is at or above maxQueues
    */
   public boolean isQueueLimitReached() {
      if (overQueue) {
         return overQueue;
      }

      if (parent != null) {
         return parent.isAddressLimitReached();
      }

      return false;
   }

   /**
    * Check if this quota has any limits configured.
    *
    * @return true if at least one limit (bytes, addresses, or queues) is configured
    */
   public boolean hasLimits() {
      Long bytesLimit = maxMessageBytes;
      Integer addrLimit = maxAddresses;
      Integer qLimit = maxQueues;
      return (bytesLimit != null && bytesLimit >= 0) ||
             (addrLimit != null && addrLimit >= 0) ||
             (qLimit != null && qLimit >= 0);
   }

   /**
    * Get percentage of byte limit used.
    *
    * @return percentage (0-100) or -1 if no limit configured
    */
   public double getByteUtilizationPercent() {
      Long limit = maxMessageBytes;
      if (limit == null || limit <= 0) {
         return -1;
      }
      return (sizeMetric.getSize() * 100.0) / limit;
   }

   /**
    * Get percentage of address limit used.
    *
    * @return percentage (0-100) or -1 if no limit configured
    */
   public double getAddressUtilizationPercent() {
      Integer limit = maxAddresses;
      if (limit == null || limit <= 0) {
         return -1;
      }
      return (addressCount.getElements() * 100.0) / limit;
   }

   /**
    * Get percentage of queue limit used.
    *
    * @return percentage (0-100) or -1 if no limit configured
    */
   public double getQueueUtilizationPercent() {
      Integer limit = maxQueues;
      if (limit == null || limit <= 0) {
         return -1;
      }
      return (queueCount.getElements() * 100.0) / limit;
   }

   // ========================================================================
   // Address Counter Operations
   // ========================================================================

   /**
    * Check if an address can be added without exceeding limits.
    * This is a non-modifying check - use incrementAddressCount() after successful address creation.
    *
    * @return true if an address can be added, false if adding would exceed limit
    */
   public boolean canAddAddress() {
      return !overAddress;
   }

   /**
    * Increment address count and propagate to parent.
    * Should only be called after successful canAddAddress() check.
    */
   public void incrementAddressCount() {
      addressCount.incrementElement();
      logger.debug("Quota {} address count incremented to {}", name, addressCount.getElements());
   }

   /**
    * Decrement address count and propagate to parent
    */
   public void decrementAddressCount() {
      long current = addressCount.decrementElement();

      // Always propagate to parent to maintain sync
      if (parent != null) {
         parent.decrementAddressCount();
      }

      // Log warning if negative, but don't reset - let it stay negative
      // This keeps parent-child in sync and allows self-correction
      if (current < 0) {
         logger.debug("Quota {} address count is negative: {} - this indicates a double-decrement bug that should be fixed", name, current);
      } else {
         logger.debug("Quota {} address count decremented to {}", name, current);
      }
   }

   // ========================================================================
   // Queue Counter Operations
   // ========================================================================

   /**
    * Check if a queue can be added without exceeding limits.
    * This is a non-modifying check - use incrementQueueCount() after successful queue creation.
    *
    * @return true if a queue can be added, false if adding would exceed limit
    */
   public boolean canAddQueue() {
      if (overQueue) {
         return false;
      }

      if (parent != null) {
         return parent.canAddQueue();
      } else {
         return true;
      }
   }

   /**
    * Increment queue count and propagate to parent.
    * Should only be called after successful canAddQueue() check.
    */
   public void incrementQueueCount() {
      queueCount.incrementElement();
      logger.debug("Quota {} queue count incremented to {}", name, queueCount.getElements());
   }

   /**
    * Decrement queue count and propagate to parent
    */
   public void decrementQueueCount() {
      long current = queueCount.decrementElement();

      // Log warning if negative, but don't reset - let it stay negative
      // This keeps parent-child in sync and allows self-correction
      if (current < 0) {
         logger.debug("Quota {} queue count is negative: {} - this indicates a double-decrement bug that should be fixed", name, current);
      } else {
         logger.debug("Quota {} queue count decremented to {}", name, current);
      }
   }

   /**
    * Get current address count tracked by this quota.
    */
   public int getCurrentAddressCount() {
      return (int)addressCount.getElements();
   }

   /**
    * Alias for getCurrentAddressCount() for backward compatibility.
    */
   public int getAddressCount() {
      return getCurrentAddressCount();
   }

   /**
    * Get current queue count tracked by this quota.
    */
   public int getCurrentQueueCount() {
      return (int)queueCount.getElements();
   }

   /**
    * Alias for getCurrentQueueCount() for backward compatibility.
    */
   public int getQueueCount() {
      return getCurrentQueueCount();
   }

   // ========================================================================
   // Lifecycle and Internal Methods
   // ========================================================================

   // ========================================================================
   // Copy Method (for wildcard template instantiation)
   // ========================================================================

   /**
    * Create a copy of this runtime quota for wildcard template instantiation.
    * The copy has the same limits but fresh counters (starting at zero).
    * This is used when a wildcard template (e.g., "region.*") creates a specific instance (e.g., "region.us").
    * Counters are NOT copied - new instance starts at zero and will be rebuilt by scanning.
    */
   public ResourceQuota copy(String newName) {
      ResourceQuota copy = new ResourceQuota(newName);
      copy.maxMessageBytes = this.maxMessageBytes;
      copy.maxAddresses = this.maxAddresses;
      copy.maxQueues = this.maxQueues;
      return copy;
   }

   // ========================================================================
   // Object Methods (equals, hashCode, toString)
   // ========================================================================

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof ResourceQuota that)) {
         return false;
      }
      return Objects.equals(name, that.name) &&
             Objects.equals(maxMessageBytes, that.maxMessageBytes) &&
             Objects.equals(maxAddresses, that.maxAddresses) &&
             Objects.equals(maxQueues, that.maxQueues);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, maxMessageBytes, maxAddresses, maxQueues);
   }

   @Override
   public String toString() {
      return "ResourceQuota{" +
             "name='" + name + '\'' +
             ", maxMessageBytes=" + maxMessageBytes +
             ", maxAddresses=" + maxAddresses +
             ", maxQueues=" + maxQueues +
             ", currentSize=" + sizeMetric.getSize() +
             ", currentAddresses=" + addressCount.getElements() +
             ", currentQueues=" + queueCount.getElements() + '}';
   }
}
