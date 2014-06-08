package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.annotations.ColumnName;
import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.value.Value;

public class ValueMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Value value;
    private final ValueConverter converter;
    private final boolean identifier;
    private final String columnName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ValueMapping(Value value, ValueConverter converter) {
        this.value = value;
        this.converter = converter;
        this.identifier = value.getAnnotation(Id.class) != null;
        this.columnName = columnName(value);
    }

    private static String columnName(Value value) {
        ColumnName annot = value.getAnnotation(ColumnName.class);
        return annot == null ? value.getName() : annot.value();
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

    public Value getValue() {
        return value;
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
