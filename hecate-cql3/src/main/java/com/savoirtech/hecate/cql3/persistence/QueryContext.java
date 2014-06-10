package com.savoirtech.hecate.cql3.persistence;

public interface QueryContext {
    void addPojo(Class<?> pojoType, String tableName, Object identifier, Object pojo);
}
