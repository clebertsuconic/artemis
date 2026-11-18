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
package org.apache.activemq.artemis.core.server.quota.impl;

import org.apache.activemq.artemis.api.core.ActiveMQResourceQuotaExceededException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.quota.ResourceQuotaManager;
import org.apache.activemq.artemis.core.server.quota.ResourceQuotaService;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Implementation of ResourceQuotaService with lazy initialization.
 */
public class ResourceQuotaServiceImpl implements ResourceQuotaService {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;
   private final org.apache.activemq.artemis.core.config.Configuration configuration;
   private final java.util.concurrent.ConcurrentHashMap<SimpleString, String> addressQuotaMapping = new java.util.concurrent.ConcurrentHashMap<>();
   private volatile ResourceQuotaManager resourceQuotaManager;
   private org.apache.activemq.artemis.core.postoffice.PostOffice postOffice;
   private org.apache.activemq.artemis.core.paging.PagingManager pagingManager;
   private org.apache.activemq.artemis.core.server.management.ManagementService managementService;

   public ResourceQuotaServiceImpl(HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                   org.apache.activemq.artemis.core.config.Configuration configuration) {
      this.addressSettingsRepository = addressSettingsRepository;
      this.configuration = configuration;
   }

   @Override
   public void setPostOffice(org.apache.activemq.artemis.core.postoffice.PostOffice postOffice) {
      this.postOffice = postOffice;
   }

   @Override
   public void setPagingManager(org.apache.activemq.artemis.core.paging.PagingManager pagingManager) {
      this.pagingManager = pagingManager;
   }

   @Override
   public void setManagementService(org.apache.activemq.artemis.core.server.management.ManagementService managementService) {
      this.managementService = managementService;
   }

   /**
    * Initialize ResourceQuotaManager lazily with runtime quota instances from configuration.
    * Creates ResourceQuota instances from ResourceQuotaConfig definitions.
    * Counters start at zero and are rebuilt by scanning existing addresses/queues during broker startup.
    * This is called on first access to ensure the manager is ready.
    */
   private void ensureInitialized() {
      if (resourceQuotaManager != null) {
         return;
      }

      synchronized (this) {
         if (resourceQuotaManager != null) {
            return;
         }
         createAndInitializeQuotaManager();
      }
   }

   /**
    * Create ResourceQuotaManager and initialize it with runtime quota instances from configuration.
    * Must be called within synchronized block.
    */
   private void createAndInitializeQuotaManager() {
      if (configuration == null) {
         return;
      }

      java.util.Map<String, org.apache.activemq.artemis.core.settings.impl.ResourceQuotaConfig> quotaConfigs =
         configuration.getResourceQuotas();
      if (quotaConfigs == null || quotaConfigs.isEmpty()) {
         logger.debug("No quota configurations found, skipping ResourceQuotaManager creation");
         return;
      }

      // Create ResourceQuotaManager
      org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository<ResourceQuota> quotaRepo =
         new org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository<>(
            configuration.getWildcardConfiguration());

      resourceQuotaManager = new ResourceQuotaManager(quotaRepo, configuration.getWildcardConfiguration());

      // Create runtime ResourceQuota instances from configuration
      for (java.util.Map.Entry<String, org.apache.activemq.artemis.core.settings.impl.ResourceQuotaConfig> entry : quotaConfigs.entrySet()) {
         ResourceQuota runtimeQuota = entry.getValue().createRuntimeQuota();
         resourceQuotaManager.addQuota(entry.getKey(), runtimeQuota);
      }

      // Establish parent relationships between runtime quota instances
      resourceQuotaManager.establishParentRelationships();

      // Register quotas in JMX
      if (managementService != null) {
         for (ResourceQuota quota : resourceQuotaManager.getAllQuotas()) {
            try {
               managementService.registerResourceQuota(quota);
            } catch (Exception e) {
               logger.warn("Failed to register resource quota '{}' in JMX: {}", quota.getName(), e.getMessage(), e);
            }
         }
      }
   }

   @Override
   public ResourceQuotaManager getResourceQuotaManager() {
      ensureInitialized();
      return resourceQuotaManager;
   }

   @Override
   public void checkAddressQuota(SimpleString address) throws Exception {
      ResourceQuota quota = lookupQuota(address);
      if (quota == null) {
         return;
      }

      if (!quota.canAddAddress()) {
         throw new ActiveMQResourceQuotaExceededException(
            "Address quota exceeded for quota '" + quota.getName() +
            "': max addresses is " + quota.getMaxAddresses());
      }
   }

   @Override
   public void incrementAddressCount(SimpleString address) {
      ResourceQuota quota = lookupQuota(address);
      if (quota != null) {
         quota.incrementAddressCount();
         addressQuotaMapping.put(address, quota.getName());
      }
   }

   @Override
   public void decrementAddressCount(SimpleString address) {
      ResourceQuota quota = lookupQuota(address);
      if (quota != null) {
         quota.decrementAddressCount();
         addressQuotaMapping.remove(address);
      }
   }

   @Override
   public void checkQueueQuota(SimpleString address) throws Exception {
      ResourceQuota quota = lookupQuota(address);
      if (quota == null) {
         return;
      }

      if (!quota.canAddQueue()) {
         throw new ActiveMQResourceQuotaExceededException(
            "Queue quota exceeded for quota '" + quota.getName() +
            "': max queues is " + quota.getMaxQueues());
      }
   }

   @Override
   public void incrementQueueCount(SimpleString address) {
      ResourceQuota quota = lookupQuota(address);
      if (quota != null) {
         quota.incrementQueueCount();
      }
   }

   @Override
   public void decrementQueueCount(SimpleString address) {
      ResourceQuota quota = lookupQuota(address);
      if (quota != null) {
         quota.decrementQueueCount();
      }
   }

   @Override
   public ResourceQuota lookupQuota(SimpleString address) {
      if (resourceQuotaManager == null) {
         return null;
      }

      AddressSettings settings = addressSettingsRepository.getMatch(address.toString());
      return resourceQuotaManager.getQuotaForAddress(address, settings);
   }

   @Override
   public void reloadQuotas() {
      // TODO: reload quotas should be a remove / re-add...
   }

}
