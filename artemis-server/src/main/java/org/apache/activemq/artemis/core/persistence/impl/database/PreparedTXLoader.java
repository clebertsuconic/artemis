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

package org.apache.activemq.artemis.core.persistence.impl.database;

import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.XidCodecSupport;
import org.apache.artemis.database.DatabaseProvider;
import org.apache.artemis.database.data.PrepareTXData;
import org.apache.artemis.database.queries.PreparedTXQuery;

public class PreparedTXLoader {

   public void loadPreparedTransactions(DatabaseProvider dbProvider, Connection connection) throws Exception {
      PreparedTXQuery txQuery = new PreparedTXQuery(dbProvider, connection);
      ArrayList<PrepareTXData> list = new ArrayList<>();
      txQuery.query(list::add);

      if (list.isEmpty()) {
         return;
      }

      Map<Long, TransactionImpl> transactionMap = new HashMap<>();
      for (PrepareTXData data : list) {
         Xid xid = XidCodecSupport.decodeXid(data.xid);
         System.out.println("XID::" + xid);
      }
   }

}
