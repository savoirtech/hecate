package com.savoirtech.hecate.cql3.type.natives;

import com.savoirtech.hecate.cql3.type.ColumnType;

public abstract class NativeType<T> implements ColumnType<T> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    @SuppressWarnings("unchecked")
    public T fromCassandraValue(Object cassandraValue) {
        return (T) cassandraValue;
    }

    @Override
    public Object toCassandraValue(T value) {
        return value;
    }
}
