package com.savoirtech.hecate.cql3.type;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;


public interface ColumnType<T> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Returns the {@link com.datastax.driver.core.DataType} for this column.
     * @return the {@link com.datastax.driver.core.DataType} for this column
     */
    DataType getDataType();

    /**
     * Gets the specified column value.  Subclasses must perform the conversion from the Cassandra-supported type
     * obtained by calling the getter on the {@link com.datastax.driver.core.Row}.
     * @param row the row
     * @param columnIndex the column index
     * @return the column value
     */
    T getValue(Row row, int columnIndex);
    
    /**
     * Sets the specified parameter value.  Subclasses must perform the conversion to a Cassandra-supported type in
     * order to call the proper setter on the {@link com.datastax.driver.core.BoundStatement}.
     * @param statement the statement
     * @param parameterIndex the parameter index
     * @param value the parameter value
     */
    void setValue(BoundStatement statement, int parameterIndex, T value);
}
