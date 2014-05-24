package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.NullSafeColumnType;

import java.util.Date;

public class DateType extends NullSafeColumnType<Date> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType.Name getCassandraType() {
        return DataType.Name.TIMESTAMP;
    }

    @Override
    public Date
    getValue(Row row, int columnIndex) {
        return row.getDate(columnIndex);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void nullSafeSet(BoundStatement statement, int parameterIndex, Date value) {
        statement.setDate(parameterIndex, value);
    }
}
