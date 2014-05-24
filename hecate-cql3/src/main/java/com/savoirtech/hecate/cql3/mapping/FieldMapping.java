package com.savoirtech.hecate.cql3.mapping;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public interface FieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void bindTo(Object root, BoundStatement statement, int parameterIndex);

    DataType columnType();

    void extractFrom(Object root, Row row, int columnIndex);
}
