package com.savoirtech.hecate.cql3.type;

import com.datastax.driver.core.BoundStatement;

public abstract class NullSafeColumnType<T> implements ColumnType<T> {
//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract void nullSafeSet(BoundStatement statement, int parameterIndex, T value);

//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public final void setValue(BoundStatement statement, int parameterIndex, T value) {
        if (value != null) {
            nullSafeSet(statement, parameterIndex, value);
        }
    }
}
