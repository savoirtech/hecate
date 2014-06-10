package com.savoirtech.hecate.cql3.mapping;

public interface PojoMappingFactory {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    PojoMapping getPojoMapping(Class<?> pojoType, String tableName);
}
