package com.savoirtech.hecate.cql3.handler;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.Arrays;

public class SimpleHandler extends AbstractColumnHandler<Object, Object> {
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
    protected Object convertToFacetValue(Object columnValue, ValueConverter converter) {
        return converter.fromCassandraValue(columnValue);
    }

    @Override
    protected Iterable<Object> toColumnValues(Object columnValue) {
        return Arrays.asList(columnValue);
    }
}
