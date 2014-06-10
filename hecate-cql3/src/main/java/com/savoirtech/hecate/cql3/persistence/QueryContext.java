package com.savoirtech.hecate.cql3.persistence;

import com.savoirtech.hecate.cql3.meta.PojoMetadata;

import java.util.Map;

public interface QueryContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void addPojo(Class<?> pojoType, String tableName, Object identifier, Object pojo);

    void addPojos(Class<?> pojoType, String tableName, Map<Object, Object> pojoMap);

    Object newPojo(PojoMetadata pojoMetadata, Object identifier);

    Map<Object, Object> newPojoMap(PojoMetadata pojoMetadata, Iterable<Object> identifiers);
}
