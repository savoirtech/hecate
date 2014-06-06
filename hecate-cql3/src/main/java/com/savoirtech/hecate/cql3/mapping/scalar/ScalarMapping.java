package com.savoirtech.hecate.cql3.mapping.scalar;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.mapping.AbstractFieldMapping;
import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.util.CassandraUtils;

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


    @Override
    public DataType columnType() {
        return columnType.getDataType();
    }

    @Override
    public Object fieldCassandraValue(Object pojo) {
        return columnType.toCassandraValue(getFieldValue(pojo));
    }

    public void populateFromRow(Object root, Row row, int columnIndex) {
        final Object value = CassandraUtils.getValue(row, columnIndex, columnType.getDataType());
        setFieldValue(root, columnType.fromCassandraValue(value));
    }

    @Override
    public Object rawCassandraValue(Object rawValue) {
        return columnType.toCassandraValue(rawValue);
    }
}
