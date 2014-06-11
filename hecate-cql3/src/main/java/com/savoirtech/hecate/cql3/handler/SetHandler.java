package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.HashSet;
import java.util.Set;

public class SetHandler extends AbstractColumnHandler<Set<Object>, Set<Object>> {
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
    public Set<Object> getInsertValue(Set<Object> facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        return copyFacetValues(facetValue, new HashSet<>(), context);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Set<Object> convertToFacetValue(Set<Object> columnValue, ValueConverter converter) {
        if (columnValue == null) {
            return null;
        }
        return copyColumnValues(columnValue, new HashSet<>(), converter);
    }

    @Override
    protected Iterable<Object> toColumnValues(Set<Object> columnValue) {
        return columnValue;
    }
}
