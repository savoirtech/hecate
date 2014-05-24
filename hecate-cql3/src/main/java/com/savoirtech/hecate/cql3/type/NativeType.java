package com.savoirtech.hecate.cql3.type;

import com.datastax.driver.core.DataType;

public class NativeType<T> implements ColumnType<T> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final DataType dataType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public NativeType(DataType dataType) {
        this.dataType = dataType;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public T fromCassandraValue(Object cassandraValue) {
        return (T) cassandraValue;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public Object toCassandraValue(T value) {
        return value;
    }
}
