package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.HashMap;
import java.util.Map;

public class MapHandler extends AbstractColumnHandler<Map<Object, Object>, Map<Object, Object>> {
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
    public Map<Object, Object> getInsertValue(Map<Object, Object> facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        Map<Object, Object> columnValue = new HashMap<>();
        for (Map.Entry<Object, Object> entry : facetValue.entrySet()) {
            columnValue.put(keyConverter.toCassandraValue(entry.getKey()),
                    getDelegate().convertToInsertValue(entry.getValue(), context));
        }
        return columnValue;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Map<Object, Object> convertToFacetValue(Map<Object, Object> columnValue, ValueConverter converter) {
        if (columnValue == null) {
            return null;
        }
        Map<Object, Object> facetValues = new HashMap<>();
        for (Map.Entry<Object, Object> entry : columnValue.entrySet()) {
            facetValues.put(keyConverter.fromCassandraValue(entry.getKey()), converter.fromCassandraValue(entry.getValue()));
        }
        return facetValues;
    }

    @Override
    protected Iterable<Object> toColumnValues(Map<Object, Object> columnValue) {
        return columnValue.values();
    }
}
