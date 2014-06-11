package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListHandler extends AbstractColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ListHandler(ColumnHandlerDelegate delegate) {
        super(delegate, DataType.list(delegate.getDataType()));
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
        List<Object> facetValues = (List<Object>) facetValue;
        List<Object> columnValues = new ArrayList<>();
        for (Object value : facetValues) {
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
        List<Object> facetValues = new ArrayList<>(columnValues.size());
        for (Object value : columnValues) {
            facetValues.add(conversions.get(value));
        }
        return facetValues;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterable<Object> toColumnValues(Object columnValue) {
        return (List<Object>) columnValue;
    }
}
