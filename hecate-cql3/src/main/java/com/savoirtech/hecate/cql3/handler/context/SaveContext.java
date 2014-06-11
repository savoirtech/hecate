package com.savoirtech.hecate.cql3.handler.context;

public interface SaveContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void addPojo(Class<?> pojoType, String tableName, Object identifier, Object pojo);
}
