package com.savoirtech.hecate.cql3.persistence;

public interface Hydrator {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    <P> P newPojo(Class<P> pojoType, Object identifier);
}
