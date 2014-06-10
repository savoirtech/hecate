package com.savoirtech.hecate.cql3.handler;

import com.savoirtech.hecate.cql3.meta.FacetMetadata;

public interface ColumnHandlerFactory {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    ColumnHandler getColumnHandler(FacetMetadata facetMetadata);
}
