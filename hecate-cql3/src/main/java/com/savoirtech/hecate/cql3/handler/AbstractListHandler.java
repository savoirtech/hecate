package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractListHandler extends AbstractColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected final DataType columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractListHandler(DataType elementType) {
        this.columnType = DataType.list(elementType);
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
        List<Object> cassandraValues = (List<Object>) cassandraValue;
        List<Object> facetValues = new ArrayList<>(cassandraValues.size());
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
        List<Object> facetValues = (List<Object>) facetValue;
        List<Object> cassandraValues = new ArrayList<>(facetValues.size());
        for (Object value : facetValues) {
            cassandraValues.add(toCassandraElement(value, context));
        }
        onInsertValueComplete(cassandraValues, context);
        return cassandraValues;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void onFacetValueComplete(List<Object> facetValues, QueryContext context) {
    }

    protected void onInsertValueComplete(List<Object> cassandraValues, SaveContext context) {

    }
}
