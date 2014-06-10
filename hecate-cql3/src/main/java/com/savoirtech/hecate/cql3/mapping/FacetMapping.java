package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.handler.ColumnHandler;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;

public class FacetMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final FacetMetadata facet;
    private final ColumnHandler columnHandler;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FacetMapping(FacetMetadata facet, ColumnHandler columnHandler) {
        this.facet = facet;
        this.columnHandler = columnHandler;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public ColumnHandler getColumnHandler() {
        return columnHandler;
    }

    public FacetMetadata getFacetMetadata() {
        return facet;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(facet.getColumnName());
        sb.append(" ");
        sb.append(columnHandler.getColumnType());
        if (facet.isIdentifier()) {
            sb.append(" PRIMARY KEY");
        }
        return sb.toString();
    }
}
