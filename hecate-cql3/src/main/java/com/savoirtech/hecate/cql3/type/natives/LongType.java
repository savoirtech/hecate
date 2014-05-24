package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.NullSafeColumnType;

public class LongType extends NullSafeColumnType<Long> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataType.bigint();
    }

    @Override
    public Long getValue(Row row, int columnIndex) {
        return row.getLong(columnIndex);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void nullSafeSet(BoundStatement statement, int parameterIndex, Long value) {
        statement.setLong(parameterIndex, value);
    }
}
