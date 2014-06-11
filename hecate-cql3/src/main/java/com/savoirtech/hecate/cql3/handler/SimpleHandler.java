package com.savoirtech.hecate.cql3.handler;

import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.Arrays;
import java.util.Map;

public class SimpleHandler extends AbstractColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SimpleHandler(ColumnHandlerDelegate delegate) {
        super(delegate, delegate.getDataType());
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object getInsertValue(Object facetValue, SaveContext context) {
        return facetValue == null ? null : getDelegate().convertToInsertValue(facetValue, context);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object convertToFacetValue(Object columnValue, Map<Object, Object> conversions) {
        return conversions.get(columnValue);
    }

    @Override
    protected Iterable<Object> toColumnValues(Object columnValue) {
        return Arrays.asList(columnValue);
    }
}
