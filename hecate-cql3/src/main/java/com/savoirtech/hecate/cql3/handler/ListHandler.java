package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.ArrayList;
import java.util.List;

public class ListHandler extends AbstractColumnHandler<List<Object>, List<Object>> {
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
    public List<Object> getInsertValue(List<Object> facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        return copyFacetValues(facetValue, new ArrayList<>(facetValue.size()), context);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected List<Object> convertToFacetValue(List<Object> columnValues, ValueConverter converter) {
        if (columnValues == null) {
            return null;
        }
        return copyColumnValues(columnValues, new ArrayList<>(columnValues.size()), converter);
    }

    @Override
    protected Iterable<Object> toColumnValues(List<Object> columnValue) {
        return columnValue;
    }
}
