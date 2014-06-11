package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SetHandler extends AbstractColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SetHandler(ColumnHandlerDelegate delegate) {
        super(delegate, DataType.set(delegate.getDataType()));
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Object getInsertValue(Object facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        Set<Object> facetValues = (Set<Object>) facetValue;
        Set<Object> columnValues = new HashSet<>();
        for (Object value : facetValues) {
            columnValues.add(delegate.convertToInsertValue(value, context));
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
        Set<Object> columnValues = (Set<Object>) columnValue;
        Set<Object> facetValues = new HashSet<>();
        for (Object value : columnValues) {
            facetValues.add(conversions.get(value));
        }
        return facetValues;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterable<Object> toColumnValues(Object columnValue) {
        return (Set<Object>) columnValue;
    }
}
