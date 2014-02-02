/*
 * Copyright (c) 2012. Latinus S.A.
 */

package com.savoirtech.hecate.core.config;

public class ObjectNotSerializableException extends RuntimeException {

    public ObjectNotSerializableException() {
    }

    public ObjectNotSerializableException(String message) {
        super(message);
    }

    public ObjectNotSerializableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ObjectNotSerializableException(Throwable cause) {
        super(cause);
    }
}
