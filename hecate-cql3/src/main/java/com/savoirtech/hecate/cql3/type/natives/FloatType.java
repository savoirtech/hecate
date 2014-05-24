package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.NullSafeColumnType;

public class FloatType extends NullSafeColumnType<Float> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataType.cfloat();
    }

    @Override
    public Float getValue(Row row, int columnIndex) {
        return row.getFloat(columnIndex);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void nullSafeSet(BoundStatement statement, int parameterIndex, Float value) {
        statement.setFloat(parameterIndex, value);
    }
}
