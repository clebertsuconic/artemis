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
package org.apache.activemq.artemis.protocol.amqp.broker;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeBody;
import org.apache.qpid.proton.codec.ReadableBuffer;

/**
 * An AMQP large message variant that accumulates body bytes in memory
 * instead of writing to a file. Used by database storage where large
 * message content is stored inline as a blob.
 */
public class DatabaseAMQPLargeMessage extends AMQPLargeMessage {

   private ByteBuf accumulatedBytes;
   private volatile AMQPStandardMessage convertedMessage;

   public DatabaseAMQPLargeMessage(long id,
                                   long messageFormat,
                                   CoreMessageObjectPools coreMessageObjectPools,
                                   StorageManager storageManager) {
      super(id, messageFormat, null, coreMessageObjectPools, storageManager, new LargeBody(null, null));
      getLargeBody().setMessage(this);
      getLargeBody().setStorageManager(storageManager);
   }

   @Override
   public void addBytes(ReadableBuffer data) throws Exception {
      parseLargeMessage(data);

      final int remaining = data.remaining();
      if (accumulatedBytes == null) {
         accumulatedBytes = Unpooled.buffer(remaining);
      }
      accumulatedBytes.ensureWritable(remaining);
      byte[] chunk = new byte[remaining];
      data.get(chunk);
      accumulatedBytes.writeBytes(chunk);
   }

   @Override
   public void addBytes(byte[] bytes) throws Exception {
      if (accumulatedBytes == null) {
         accumulatedBytes = Unpooled.buffer(bytes.length);
      }
      accumulatedBytes.writeBytes(bytes);
   }

   @Override
   public void addBytes(ActiveMQBuffer bytes, boolean initialHeader) throws Exception {
      final int readableBytes = bytes.readableBytes();
      if (accumulatedBytes == null) {
         accumulatedBytes = Unpooled.buffer(readableBytes);
      }
      byte[] chunk = new byte[readableBytes];
      bytes.readBytes(chunk);
      accumulatedBytes.writeBytes(chunk);
   }

   @Override
   public ReadableBuffer getData() {
      if (accumulatedBytes != null && accumulatedBytes.readableBytes() > 0) {
         byte[] data = new byte[accumulatedBytes.readableBytes()];
         accumulatedBytes.getBytes(0, data);
         return ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(data));
      }
      return ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.allocate(0));
   }

   @Override
   public Message toMessage() {
      if (convertedMessage != null) {
         return convertedMessage;
      }

      if (accumulatedBytes != null && accumulatedBytes.readableBytes() > 0) {
         byte[] data = new byte[accumulatedBytes.readableBytes()];
         accumulatedBytes.getBytes(0, data);
         AMQPStandardMessage standard = new AMQPStandardMessage(messageFormat, data, null, coreMessageObjectPools);
         standard.setMessageID(getMessageID());
         standard.setAddress(getAddress());
         if (isDurable()) {
            standard.setDurable(true);
         }
         if (getExpiration() > 0) {
            standard.reloadExpiration(getExpiration());
         }
         standard.setMessageAnnotations(messageAnnotations);
         if (priority != AMQPMessage.DEFAULT_MESSAGE_PRIORITY) {
            standard.setPriority(priority);
         }
         convertedMessage = standard;
         return standard;
      }

      return this;
   }

   @Override
   public Persister<Message> getPersister() {
      Message converted = toMessage();
      if (converted == this) {
         return super.getPersister();
      }
      return converted.getPersister();
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      Message converted = toMessage();
      if (converted == this) {
         super.persist(targetRecord);
         return;
      }
      converted.persist(targetRecord);
   }

   @Override
   public int getPersistSize() {
      Message converted = toMessage();
      if (converted == this) {
         return super.getPersistSize();
      }
      return converted.getPersistSize();
   }

   @Override
   public long getPersistentSize() {
      return accumulatedBytes != null ? accumulatedBytes.readableBytes() : 0;
   }

   @Override
   public void releaseResources(boolean sync, boolean sendEvent) {
   }

   @Override
   public boolean isOpen() {
      return false;
   }

   @Override
   public void deleteFile() throws Exception {
      accumulatedBytes = null;
   }

   @Override
   public SequentialFile getAppendFile() throws ActiveMQException {
      return null;
   }

   @Override
   public LargeBodyReader getLargeBodyReader() {
      return new LargeBodyReader() {
         private boolean open = false;
         private int position = 0;

         @Override
         public void open() {
            open = true;
            position = 0;
         }

         @Override
         public void close() {
            open = false;
         }

         @Override
         public void position(long newPosition) {
            position = (int) newPosition;
         }

         @Override
         public long position() {
            return position;
         }

         @Override
         public long getSize() {
            return accumulatedBytes != null ? accumulatedBytes.readableBytes() : 0;
         }

         @Override
         public int readInto(ByteBuffer bufferRead) {
            if (accumulatedBytes == null) {
               return 0;
            }
            int readable = Math.min(bufferRead.remaining(), accumulatedBytes.readableBytes() - position);
            accumulatedBytes.getBytes(position, bufferRead.array(), bufferRead.arrayOffset() + bufferRead.position(), readable);
            bufferRead.position(bufferRead.position() + readable);
            position += readable;
            return readable;
         }
      };
   }

   @Override
   public Message copy() {
      return toMessage().copy();
   }

   @Override
   public Message copy(final long newID) {
      return toMessage().copy(newID);
   }

   @Override
   public Message copy(final long newID, boolean isDLQOrExpiry) {
      return toMessage().copy(newID);
   }
}
