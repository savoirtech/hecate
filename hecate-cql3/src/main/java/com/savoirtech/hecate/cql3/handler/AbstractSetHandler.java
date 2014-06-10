package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractSetHandler implements ColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected final DataType columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractSetHandler(DataType elementType) {
        this.columnType = DataType.set(elementType);
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object toCassandraElement(Object facetElement, SaveContext context);

    protected abstract Object toFacetElement(Object cassandraElement, QueryContext context);

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getColumnType() {
        return columnType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getFacetValue(Object cassandraValue, QueryContext context) {
        if (cassandraValue == null) {
            return null;
        }
        Set<Object> cassandraValues = (Set<Object>) cassandraValue;
        Set<Object> facetValues = new HashSet<>();
        for (Object value : cassandraValues) {
            facetValues.add(toFacetElement(value, context));
        }
        onFacetValueComplete(facetValues, context);
        return facetValues;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getInsertValue(Object facetValue, SaveContext context) {
        if (facetValue == null) {
            return null;
        }
        Set<Object> facetValues = (Set<Object>) facetValue;
        Set<Object> cassandraValues = new HashSet<>();
        for (Object value : facetValues) {
            cassandraValues.add(toCassandraElement(value, context));
        }
        onInsertValueComplete(cassandraValues, context);
        return cassandraValues;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void onFacetValueComplete(Set<Object> facetValues, QueryContext context) {

    }

    private void onInsertValueComplete(Set<Object> cassandraValues, SaveContext context) {

    }
}
