package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class BooleanType extends NativeType<Boolean> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataType.cboolean();
    }

    @Override
    public Boolean extractValue(Row row, int columnIndex) {
        return row.getBool(columnIndex);
    }
}
