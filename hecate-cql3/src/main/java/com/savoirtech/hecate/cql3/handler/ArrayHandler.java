package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArrayHandler extends AbstractColumnHandler {
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
    public Object getInsertValue(Object facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        final int length = Array.getLength(facetValue);
        final List<Object> columnValues = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            final Object value = Array.get(facetValue, i);
            columnValues.add(getDelegate().convertToInsertValue(value, context));
        }
        return columnValues;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    protected Object convertToFacetValue(Object columnValue, Map<Object, Object> conversions) {
        if (columnValue == null) {
            return null;
        }
        List<Object> columnValues = (List<Object>) columnValue;
        Object array = Array.newInstance(elementType, columnValues.size());
        int index = 0;
        for (Object value : columnValues) {
            Array.set(array, index, conversions.get(value));
            index++;
        }
        return array;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterable<Object> toColumnValues(Object columnValue) {
        return (List<Object>) columnValue;
    }
}
