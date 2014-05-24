package com.savoirtech.hecate.cql3.type.natives;

import com.savoirtech.hecate.cql3.type.ColumnType;

public abstract class NativeType<T> implements ColumnType<T> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object cassandraValue(T value) {
        return value;
    }
}
