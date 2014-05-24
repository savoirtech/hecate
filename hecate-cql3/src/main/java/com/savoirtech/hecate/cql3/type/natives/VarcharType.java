package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class VarcharType extends NativeType<String> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public String extractValue(Row row, int columnIndex) {
        return row.getString(columnIndex);
    }

    @Override
    public DataType getDataType() {
        return DataType.varchar();
    }
}
