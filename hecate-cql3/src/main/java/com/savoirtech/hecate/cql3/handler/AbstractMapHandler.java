package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMapHandler extends AbstractColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected final DataType columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractMapHandler(DataType keyType, DataType valueType) {
        this.columnType = DataType.map(keyType, valueType);
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object toCassandraKey(Object key);

    protected abstract Object toCassandraValue(Object value, SaveContext context);

    protected abstract Object toFacetKey(Object key);

    protected abstract Object toFacetValue(Object value, QueryContext context);

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
        Map<Object, Object> cassandraMap = (Map<Object, Object>) cassandraValue;
        Map<Object, Object> facetMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : cassandraMap.entrySet()) {
            facetMap.put(toFacetKey(entry.getKey()), toFacetValue(entry.getValue(), context));
        }
        onFacetValueComplete(facetMap, context);
        return facetMap;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getInsertValue(Object facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        Map<Object, Object> facetMap = (Map<Object, Object>) facetValue;
        Map<Object, Object> cassandraMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : facetMap.entrySet()) {
            cassandraMap.put(toCassandraKey(entry.getKey()), toCassandraValue(entry.getValue(), context));
        }
        onInsertValueComplete(cassandraMap, context);
        return cassandraMap;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void onFacetValueComplete(Map<Object, Object> facetMap, QueryContext context) {

    }

    protected void onInsertValueComplete(Map<Object, Object> cassandraMap, SaveContext context) {

    }
}
