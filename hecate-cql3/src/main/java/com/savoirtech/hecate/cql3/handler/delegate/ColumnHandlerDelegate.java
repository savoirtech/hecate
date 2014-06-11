package com.savoirtech.hecate.cql3.handler.delegate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.util.InjectionTarget;

import java.util.Map;

public interface ColumnHandlerDelegate {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    DataType getDataType();

    boolean isCascading();

    void injectFacetValues(InjectionTarget<Map<Object, Object>> target, Iterable<Object> columnValues, QueryContext context);

    void collectDeletionIdentifiers(Iterable<Object> columnValues, DeleteContext context);

    Object getWhereClauseValue(Object parameterValue);

    Object convertToInsertValue(Object facetValue, SaveContext saveContext);

}
