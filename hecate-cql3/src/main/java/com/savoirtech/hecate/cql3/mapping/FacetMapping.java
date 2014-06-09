package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.annotations.ColumnName;
import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.value.Facet;

public class FacetMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Facet facet;
    private final ValueConverter converter;
    private final boolean identifier;
    private final String columnName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FacetMapping(Facet facet, ValueConverter converter) {
        this.facet = facet;
        this.converter = converter;
        this.identifier = facet.getAnnotation(Id.class) != null;
        this.columnName = columnName(facet);
    }

    private static String columnName(Facet facet) {
        ColumnName annot = facet.getAnnotation(ColumnName.class);
        return annot == null ? facet.getName() : annot.value();
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public String getColumnName() {
        return columnName;
    }

    public ValueConverter getConverter() {
        return converter;
    }

    public Facet getFacet() {
        return facet;
    }

    public boolean isIdentifier() {
        return identifier;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(columnName);
        sb.append(" ");
        sb.append(converter.getDataType().toString());
        if (identifier) {
            sb.append(" PRIMARY KEY");
        }
        return sb.toString();

    }
}
