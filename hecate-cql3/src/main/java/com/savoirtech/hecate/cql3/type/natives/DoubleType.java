package com.savoirtech.hecate.cql3.type.natives;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.NullSafeColumnType;

public class DoubleType extends NullSafeColumnType<Double> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public DataType.Name getCassandraType() {
        return DataType.Name.DOUBLE;
    }

    @Override
    public Double getColumnValue(Row row, int columnIndex) {
        return row.getDouble(columnIndex);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void nullSafeSet(BoundStatement statement, int columnIndex, Double value) {
        statement.setDouble(columnIndex, value);
    }
}
