package com.savoirtech.hecate.cql3.handler.context;

import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.util.Callback;

import java.util.List;
import java.util.Map;

public interface QueryContext {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void addPojos(Class<?> pojoType, String tableName, Iterable<Object> identifiers, Callback<List<Object>> target);

    Map<Object, Object> newPojoMap(PojoMetadata pojoMetadata, Iterable<Object> identifiers);
}
