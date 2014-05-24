package com.savoirtech.hecate.cql3.type;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;


public interface ColumnType<T> {
    void setColumnValue(BoundStatement statement, int columnIndex, T value);
    T getColumnValue(Row row, int columnIndex);
    DataType.Name  getCassandraType();
}
