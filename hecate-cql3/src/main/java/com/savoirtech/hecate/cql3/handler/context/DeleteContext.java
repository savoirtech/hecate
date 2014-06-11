package com.savoirtech.hecate.cql3.handler.context;

public interface DeleteContext {
    void addDeletedIdentifiers(Class<?> pojoType, String tableName, Iterable<Object> identifiers);
}
