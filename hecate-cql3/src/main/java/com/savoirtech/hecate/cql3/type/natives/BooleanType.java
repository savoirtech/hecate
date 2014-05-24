package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.NullSafeColumnType;

public class BooleanType extends NullSafeColumnType<Boolean> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public DataType getDataType() {
        return DataType.cboolean();
    }

    @Override
    public Boolean getValue(Row row, int columnIndex) {
        return row.getBool(columnIndex);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void nullSafeSet(BoundStatement statement, int parameterIndex, Boolean value) {
        statement.setBool(parameterIndex, value);
    }
}
