package org.apache.activemq.artemis.utils.collections;

import java.util.Iterator;

public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {

   void close();
}
