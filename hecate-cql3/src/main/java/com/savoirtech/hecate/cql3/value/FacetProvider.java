package com.savoirtech.hecate.cql3.value;

import java.util.List;

public interface FacetProvider {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    List<Facet> getFacets(Class<?> pojoType);
}
