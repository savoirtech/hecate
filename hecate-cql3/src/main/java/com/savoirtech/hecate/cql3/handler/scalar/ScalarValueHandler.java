package com.savoirtech.hecate.cql3.handler.scalar;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.ColumnHandler;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

public class ScalarValueHandler implements ColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter converter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarValueHandler(ValueConverter converter) {
        this.converter = converter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public DataType getColumnType() {
        return converter.getDataType();
    }

    @Override
    public Object getInsertValue(Object facetValue, SaveContext context) {
        return converter.toCassandraValue(facetValue);
    }

    @Override
    public Object getFacetValue(Object cassandraValue, QueryContext context) {
        return converter.fromCassandraValue(cassandraValue);
    }

    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return converter.toCassandraValue(parameterValue);
    }
}
