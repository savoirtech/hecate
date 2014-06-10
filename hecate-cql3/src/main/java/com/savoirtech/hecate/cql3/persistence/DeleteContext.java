package com.savoirtech.hecate.cql3.persistence;

public interface DeleteContext {
    void addDeletedIdentifiers(Class<?> pojoType, String tableName, Iterable<Object> identifiers);
}
