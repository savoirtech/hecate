package com.savoirtech.hecate.cql3.persistence;

import java.util.Iterator;
import java.util.List;

public interface PojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Iterator<P> iterate(Object... parameters);

    List<P> list(Object... parameters);

    P one(Object... parameters);
}
