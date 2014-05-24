package com.savoirtech.hecate.cql3.mapping.array;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.mapping.AbstractFieldMapping;
import com.savoirtech.hecate.cql3.type.ColumnType;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ArrayFieldMapping extends AbstractFieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnType<Object> columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayFieldMapping(Field field, ColumnType<Object> columnType) {
        super(field);
        this.columnType = columnType;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType columnType() {
        return DataType.list(columnType.getDataType());
    }

    @Override
    public Object fieldCassandraValue(Object pojo) {
        final Object fieldValue = getFieldValue(pojo);
        return fieldValue == null ? null : toCassandraValues(fieldValue);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void populateFromRow(Object root, Row row, int columnIndex) {
        final List<Object> list = (List<Object>) row.getList(columnIndex, columnType.getDataType().asJavaClass());
        if (list == null) {
            setFieldValue(root, null);
        } else {
            final Object array = Array.newInstance(getFieldType().getComponentType(), list.size());
            final int length = Array.getLength(array);
            for (int i = 0; i < length; ++i) {
                Array.set(array, i, columnType.fromCassandraValue(list.get(i)));
            }
            setFieldValue(root, array);
        }
    }

    @Override
    public Object rawCassandraValue(Object value) {
        return toCassandraValues(value);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Object toCassandraValues(Object fieldValue) {
        final List<Object> elements = toList(fieldValue);
        final List<Object> cassandraValues = new ArrayList<>(elements.size());
        for (Object originalValue : elements) {
            cassandraValues.add(columnType.toCassandraValue(originalValue));
        }
        return cassandraValues;
    }

    private List<Object> toList(Object array) {
        final int length = Array.getLength(array);
        final List<Object> elements = new ArrayList<Object>(length);
        for (int i = 0; i < length; ++i) {
            elements.add(Array.get(array, i));
        }
        return elements;
    }
}
