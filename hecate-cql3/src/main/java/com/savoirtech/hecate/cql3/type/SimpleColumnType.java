package com.savoirtech.hecate.cql3.type;

public abstract class SimpleColumnType implements ColumnType {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object getColumnValue(Object fieldValue) {
        return fieldValue;
    }
}
