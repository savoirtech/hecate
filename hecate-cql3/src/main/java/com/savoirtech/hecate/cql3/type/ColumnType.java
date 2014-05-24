package com.savoirtech.hecate.cql3.type;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;


public interface ColumnType<T> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Returns the {@link com.datastax.driver.core.DataType} for this column.
     *
     * @return the {@link com.datastax.driver.core.DataType} for this column
     */
    DataType getDataType();

    /**
     * Gets the specified column value.  Subclasses must perform the conversion from the Cassandra-supported type
     * obtained by calling the getter on the {@link com.datastax.driver.core.Row}.
     *
     * @param row         the row
     * @param columnIndex the column index
     * @return the column value
     */
    T extractValue(Row row, int columnIndex);

    /**
     * Converts the parameter to a value suitable to be used by Cassandra.
     *
     * @param value the original value
     * @return a value suitable to be used by Cassandra
     */
    Object toCassandraValue(T value);

    /**
     * Converts the Cassandra-supported type to the type supported by this {@link ColumnType}.
     *
     * @param cassandraValue the Cassandra-supported type
     * @return the type supported by this {@link ColumnType}
     */
    T fromCassandraValue(Object cassandraValue);
}
