package com.savoirtech.hecate.cql3.meta;

public interface PojoDescriptorFactory {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    <P> PojoDescriptor<P> getPojoDescriptor(Class<P> pojoType);
}
