package com.savoirtech.hecate.cql3.persistence;

public interface SaveContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void addPojo(Class<?> pojoType, String tableName, Object identifier, Object pojo);
}
