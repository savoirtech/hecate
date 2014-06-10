package com.savoirtech.hecate.cql3.meta;

public interface PojoMetadataFactory {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    PojoMetadata getPojoMetadata(Class<?> pojoType);
}
