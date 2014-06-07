package com.savoirtech.hecate.cql3.value;

import java.util.List;

public interface ValueProvider {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    List<Value> getValues(Class<?> pojoType);
}
