/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<E> extends Closeable, Iterator<E>
{
}
