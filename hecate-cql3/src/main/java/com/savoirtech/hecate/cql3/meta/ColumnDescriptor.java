package com.savoirtech.hecate.cql3.meta;

import com.savoirtech.hecate.cql3.mapping.FieldMapping;

public class ColumnDescriptor {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final String columnName;
    private final boolean identifier;
    private final FieldMapping mapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ColumnDescriptor(String columnName, boolean identifier, FieldMapping mapping) {
        this.columnName = columnName;
        this.identifier = identifier;
        this.mapping = mapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public String getColumnName() {
        return columnName;
    }

    public FieldMapping getMapping() {
        return mapping;
    }

    public boolean isIdentifier() {
        return identifier;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(columnName);
        sb.append(" ");
        sb.append(mapping.columnType().toString());
        if(identifier) {
            sb.append(" PRIMARY KEY");
        }
        return sb.toString();
    }
}
