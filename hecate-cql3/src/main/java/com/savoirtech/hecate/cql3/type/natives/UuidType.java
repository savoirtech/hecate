package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.NullSafeColumnType;

import java.util.UUID;

public class UuidType extends NullSafeColumnType<UUID> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataType.uuid();
    }

    @Override
    public UUID getValue(Row row, int columnIndex) {
        return row.getUUID(columnIndex);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void nullSafeSet(BoundStatement statement, int parameterIndex, UUID value) {
        statement.setUUID(parameterIndex, value);
    }
}
