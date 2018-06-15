/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */
package com.oath.halodb;

/**
 * @author Arjun Mannaly
 */
public class HaloDBException extends Exception {
    private static final long serialVersionUID = 1010101L;

    public HaloDBException(String message) {
        super(message);
    }

    public HaloDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public HaloDBException(Throwable cause) {
        super(cause);
    }
}
