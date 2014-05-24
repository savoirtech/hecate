package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class DoubleType extends NativeType<Double> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Double extractValue(Row row, int columnIndex) {
        return row.getDouble(columnIndex);
    }

    @Override
    public DataType getDataType() {
        return DataType.cdouble();
    }
}
