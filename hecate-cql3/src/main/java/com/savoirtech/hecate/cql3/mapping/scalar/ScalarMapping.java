package com.savoirtech.hecate.cql3.mapping.scalar;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.mapping.AbstractFieldMapping;
import com.savoirtech.hecate.cql3.type.ColumnType;

import java.lang.reflect.Field;

public class ScalarMapping extends AbstractFieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnType<Object> columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarMapping(Field field, ColumnType<Object> columnType) {
        super(field);
        this.columnType = columnType;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    public void bindTo(Object root, BoundStatement statement, int parameterIndex) {
        columnType.setValue(statement, parameterIndex, getFieldValue(root));
    }

    @Override
    public DataType columnType() {
        return columnType.getDataType();
    }

    public void extractFrom(Object root, Row row, int columnIndex) {
        setFieldValue(root, columnType.getValue(row, columnIndex));
    }
}
