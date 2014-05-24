package com.savoirtech.hecate.cql3.type;

import com.datastax.driver.core.BoundStatement;

public abstract class NullSafeColumnType<T> implements ColumnType<T> {
//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract void nullSafeSet(BoundStatement statement, int index, T value);

//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public final void setColumnValue(BoundStatement statement, int columnIndex, T value) {
        if (value != null) {
            nullSafeSet(statement, columnIndex, value);
        }
    }
}
