package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

import java.util.UUID;

public class UuidType extends NativeType<UUID> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public UUID extractValue(Row row, int columnIndex) {
        return row.getUUID(columnIndex);
    }

    @Override
    public DataType getDataType() {
        return DataType.uuid();
    }
}
