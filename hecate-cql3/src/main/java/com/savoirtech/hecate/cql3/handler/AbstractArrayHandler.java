package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractArrayHandler implements ColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected final Class<?> elementType;
    protected final DataType columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractArrayHandler(Class<?> elementType, DataType elementDataType) {
        this.columnType = DataType.list(elementDataType);
        this.elementType = elementType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object toCassandraElement(Object facetElement, SaveContext context);

    protected abstract Object toFacetElement(Object cassandraElement, QueryContext context);

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------


    public DataType getColumnType() {
        return columnType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getFacetValue(Object cassandraValue, QueryContext context) {
        if (cassandraValue == null) {
            return null;
        }
        List<Object> cassandraValues = (List<Object>) cassandraValue;
        Object array = Array.newInstance(elementType, cassandraValues.size());
        int index = 0;
        for (Object value : cassandraValues) {
            Array.set(array, index, toFacetElement(value, context));
            index++;
        }
        return array;
    }

    @Override
    public Object getInsertValue(Object facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        final int length = Array.getLength(facetValue);
        final List<Object> elements = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            final Object value = Array.get(facetValue, i);
            elements.add(toCassandraElement(value, context));
        }
        return elements;
    }
}
