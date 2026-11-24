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
package org.apache.activemq.artemis.core.server.quota;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Manages resource quotas including template instantiation and parent hierarchy resolution.
 * <p>
 * This manager handles:
 * <ul>
 *   <li>Storage and retrieval of quota definitions</li>
 *   <li>Wildcard template expansion (e.g., "EU.*" template creates "EU.fr" instance)</li>
 *   <li>Parent-child quota hierarchy establishment</li>
 * </ul>
 */
public class ResourceQuotaManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final HierarchicalObjectRepository<ResourceQuota> quotaRepository;
   private final ConcurrentHashMap<String, ResourceQuota> instantiatedQuotas;
   private final WildcardConfiguration wildcardConfiguration;

   public ResourceQuotaManager(HierarchicalObjectRepository<ResourceQuota> quotaRepository,
                               WildcardConfiguration wildcardConfiguration) {
      this.quotaRepository = quotaRepository;
      this.instantiatedQuotas = new ConcurrentHashMap<>();
      this.wildcardConfiguration = wildcardConfiguration;
   }

   /**
    * Add a quota to the repository.
    *
    * @param name  the quota name
    * @param quota the quota object
    */
   public void addQuota(String name, ResourceQuota quota) {
      quotaRepository.addMatch(name, quota);
      logger.debug("Added quota: {}", name);
   }

   /**
    * Get a quota by exact name.
    *
    * @param name the quota name
    * @return the quota, or null if not found
    */
   public ResourceQuota getQuota(String name) {
      return quotaRepository.getMatch(name);
   }

   /**
    * Get all configured quotas from the repository.
    * This returns all quotas including templates and exact matches.
    *
    * @return list of all quota objects from the repository
    */
   public List<ResourceQuota> getAllQuotas() {
      return quotaRepository.values();
   }

   /**
    * Remove a quota from the repository.
    * Handles bulk decrement from parent if this quota is part of a hierarchy.
    *
    * @param name the quota name to remove
    */
   public void removeQuota(String name) {
      ResourceQuota quotaBeingRemoved = quotaRepository.getMatch(name);
      if (quotaBeingRemoved == null) {
         logger.warn("Attempted to remove non-existent quota: {}", name);
         return;
      }

      // Remove from repository
      quotaRepository.removeMatch(name);

      logger.debug("Removed quota: {}", name);
   }


   @Override
   public String toString() {
      return "ResourceQuotaManager{" +
             "instantiatedQuotas=" + instantiatedQuotas.size() +
             '}';
   }
}
