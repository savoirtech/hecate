package com.savoirtech.hecate.cql3.persistence;

import java.util.Iterator;
import java.util.List;

public interface ProjectionQuery {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Iterator<List<Object>> iterate(Object... parameters);

    List<List<Object>> list(Object... parameters);

    List<Object> one(Object... parameters);
}
