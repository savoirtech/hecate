package com.savoirtech.hecate.cql3.persistence;

public interface SaveContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void enqueue(Class<?> pojoType, String tableName, Object pojo);
}
