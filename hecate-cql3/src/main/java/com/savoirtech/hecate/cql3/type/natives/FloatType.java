package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class FloatType extends NativeType<Float> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataType.cfloat();
    }

    @Override
    public Float extractValue(Row row, int columnIndex) {
        return row.getFloat(columnIndex);
    }


}
