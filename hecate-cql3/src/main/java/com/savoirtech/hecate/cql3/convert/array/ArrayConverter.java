package com.savoirtech.hecate.cql3.convert.array;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ArrayConverter implements ValueConverter {
    //----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------
    private final Class<?> elementType;
    private final ValueConverter elementConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayConverter(Class<?> elementType, ValueConverter elementConverter) {
        this.elementType = elementType;
        this.elementConverter = elementConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ValueConverter Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    @SuppressWarnings("unchecked")
    public Object fromCassandraValue(Object value) {
        if (value == null) {
            return null;
        }
        List<Object> cassandraList = (List<Object>) value;
        final Object array = Array.newInstance(elementType, cassandraList.size());
        final int length = Array.getLength(array);
        for (int i = 0; i < length; ++i) {
            Array.set(array, i, elementConverter.fromCassandraValue(cassandraList.get(i)));
        }
        return array;
    }

    @Override
    public DataType getDataType() {
        return DataType.list(elementConverter.getDataType());
    }

    @Override
    public Object toCassandraValue(Object value) {
        if (value == null) {
            return null;
        }
        final int length = Array.getLength(value);
        final List<Object> elements = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            elements.add(Array.get(value, i));
        }
        return elements;
    }
}
