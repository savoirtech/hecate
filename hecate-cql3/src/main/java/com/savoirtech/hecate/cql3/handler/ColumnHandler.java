package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

public interface ColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    DataType getColumnType();

    Object getInsertValue(Object facetValue, SaveContext context);

    Object getFacetValue(Object cassandraValue, QueryContext context);

    Object getWhereClauseValue(Object parameterValue);
}
