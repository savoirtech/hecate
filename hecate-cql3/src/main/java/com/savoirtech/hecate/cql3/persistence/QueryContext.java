package com.savoirtech.hecate.cql3.persistence;

import java.util.Map;

public interface QueryContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void addPojo(Class<?> pojoType, String tableName, Object identifier, Object pojo);

    void addPojos(Class<?> pojoType, String tableName, Map<Object, Object> pojoMap);
}
