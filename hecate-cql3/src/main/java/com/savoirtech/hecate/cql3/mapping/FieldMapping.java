package com.savoirtech.hecate.cql3.mapping;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public interface FieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Object fieldCassandraValue(Object pojo);

    DataType columnType();

    void populateFromRow(Object root, Row row, int columnIndex);

    Object rawCassandraValue(Object rawValue);
}
