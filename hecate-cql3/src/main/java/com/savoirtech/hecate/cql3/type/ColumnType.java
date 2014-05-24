package com.savoirtech.hecate.cql3.type;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public interface ColumnType {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Object getColumnValue(Object fieldValue);
    Object getFieldValue(Row row, int columnIndex);
    DataType.Name  getCassandraType();
}
