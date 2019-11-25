/*
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */
package com.oath.halodb;

/**
 * Hash entries must contain a key size at minimum.
 */
interface HashEntry {
    short getKeySize();
}

