package com.savoirtech.hecate.cql3.mapping;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.type.ColumnType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public abstract class ListBasedMapping extends AbstractFieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected final ColumnType<Object> elementType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected ListBasedMapping(Field field, ColumnType<Object> elementType) {
        super(field);
        this.elementType = elementType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object fromCassandraList(List<Object> cassandraList);

    protected abstract List<Object> toCassandraList(Object fieldValue);

//----------------------------------------------------------------------------------------------------------------------
// FieldMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType columnType() {
        return DataType.list(elementType.getDataType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void populateFromRow(Object root, Row row, int columnIndex) {
        final List<Object> list = (List<Object>) row.getList(columnIndex, elementType.getDataType().asJavaClass());
        if (list == null) {
            setFieldValue(root, null);
        } else {
            setFieldValue(root, fromCassandraList(list));
        }
    }

    @Override
    public Object rawCassandraValue(Object rawValue) {
        return rawValue == null ? null : toCassandraList(rawValue);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected List<Object> mapFromCassandraValues(List<Object> cassandraValues) {
        List<Object> values = new ArrayList<>(cassandraValues.size());
        for (Object cassandraValue : cassandraValues) {
            values.add(elementType.fromCassandraValue(cassandraValue));
        }
        return values;
    }

    protected List<Object> mapToCassandraValues(List<Object> values) {
        final List<Object> cassandraList = new ArrayList<>(values.size());
        for (Object originalValue : values) {
            cassandraList.add(elementType.toCassandraValue(originalValue));
        }
        return cassandraList;
    }
}
