package com.oath.halodb.cache;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<E> extends Closeable, Iterator<E>
{
}
