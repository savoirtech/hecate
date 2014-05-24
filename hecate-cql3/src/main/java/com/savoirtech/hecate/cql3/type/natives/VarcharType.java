package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.NullSafeColumnType;

public class VarcharType extends NullSafeColumnType<String> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public DataType getDataType() {
        return DataType.varchar();
    }

    @Override
    public String getValue(Row row, int columnIndex) {
        return row.getString(columnIndex);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void nullSafeSet(BoundStatement statement, int parameterIndex, String value) {
        statement.setString(parameterIndex, value);
    }
}
