package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.HashMap;
import java.util.Map;

public class MapHandler extends AbstractColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter keyConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public MapHandler(ValueConverter keyConverter, ColumnHandlerDelegate delegate) {
        super(delegate, DataType.map(keyConverter.getDataType(), delegate.getDataType()));
        this.keyConverter = keyConverter;
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
        Map<Object, Object> facetValues = (Map<Object, Object>) facetValue;
        Map<Object, Object> columnValues = new HashMap<>();
        for (Map.Entry<Object, Object> entry : facetValues.entrySet()) {
            columnValues.put(keyConverter.toCassandraValue(entry.getKey()),
                    getDelegate().convertToInsertValue(entry.getValue(), context));
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
        Map<Object, Object> columnValues = (Map<Object, Object>) columnValue;
        Map<Object, Object> facetValues = new HashMap<>();
        for (Map.Entry<Object, Object> entry : columnValues.entrySet()) {
            facetValues.put(keyConverter.fromCassandraValue(entry.getKey()), conversions.get(entry.getValue()));
        }
        return facetValues;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterable<Object> toColumnValues(Object columnValue) {
        return ((Map<Object, Object>) columnValue).values();
    }
}
