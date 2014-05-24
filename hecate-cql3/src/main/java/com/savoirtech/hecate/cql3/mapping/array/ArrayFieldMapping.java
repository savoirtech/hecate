package com.savoirtech.hecate.cql3.mapping.array;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.mapping.ListBasedMapping;
import com.savoirtech.hecate.cql3.type.ColumnType;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ArrayFieldMapping extends ListBasedMapping {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayFieldMapping(Field field, ColumnType<Object> columnType) {
        super(field, columnType);
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMapping Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public DataType columnType() {
        return DataType.list(elementType.getDataType());
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object fromCassandraList(List<Object> cassandraList) {
        final Object array = Array.newInstance(getFieldType().getComponentType(), cassandraList.size());
        final int length = Array.getLength(array);
        for (int i = 0; i < length; ++i) {
            Array.set(array, i, elementType.fromCassandraValue(cassandraList.get(i)));
        }
        return array;
    }

    @Override
    protected List<Object> toCassandraList(Object fieldValue) {
        final List<Object> elements = arrayToList(fieldValue);
        return mapToCassandraValues(elements);
    }

    private List<Object> arrayToList(Object array) {
        final int length = Array.getLength(array);
        final List<Object> elements = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            elements.add(Array.get(array, i));
        }
        return elements;
    }
}
