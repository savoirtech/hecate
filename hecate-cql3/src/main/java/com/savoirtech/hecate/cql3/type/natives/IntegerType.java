package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class IntegerType extends NativeType<Integer> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataType.cint();
    }

    @Override
    public Integer extractValue(Row row, int columnIndex) {
        return row.getInt(columnIndex);
    }
}
