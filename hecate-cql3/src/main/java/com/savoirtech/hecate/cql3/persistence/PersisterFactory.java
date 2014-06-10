package com.savoirtech.hecate.cql3.persistence;

public interface PersisterFactory {
    Persister getPersister(Class<?> pojoType, String tableName);
}
