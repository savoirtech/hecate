package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.util.Date;

public class DateType extends NativeType<Date> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Date
    extractValue(Row row, int columnIndex) {
        return row.getDate(columnIndex);
    }

    @Override
    public DataType getDataType() {
        return DataType.timestamp();
    }
}
