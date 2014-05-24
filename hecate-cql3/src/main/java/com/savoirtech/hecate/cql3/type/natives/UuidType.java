package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.SimpleColumnType;

public class UuidType extends SimpleColumnType {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public DataType.Name getCassandraType() {
        return DataType.Name.UUID;
    }

    @Override
    public Object getFieldValue(Row row, int columnIndex) {
        return row.getUUID(columnIndex);
    }
}
