package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ArrayHandler extends AbstractColumnHandler<List<Object>, Object> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> elementType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayHandler(Class<?> elementType, ColumnHandlerDelegate delegate) {
        super(delegate, DataType.list(delegate.getDataType()));
        this.elementType = elementType;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public List<Object> getInsertValue(Object array, SaveContext context) {
        if (array == null) {
            return null;
        }
        final int length = Array.getLength(array);
        final List<Object> columnValues = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            final Object value = Array.get(array, i);
            columnValues.add(getDelegate().convertToInsertValue(value, context));
        }
        return columnValues;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object convertToFacetValue(List<Object> columnValue, ValueConverter converter) {
        if (columnValue == null) {
            return null;
        }
        Object array = Array.newInstance(elementType, columnValue.size());
        int index = 0;
        for (Object element : columnValue) {
            Array.set(array, index, converter.fromCassandraValue(element));
            index++;
        }
        return array;
    }

    @Override
    protected Iterable<Object> toColumnValues(List<Object> columnValue) {
        return columnValue;
    }
}
