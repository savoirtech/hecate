package com.savoirtech.hecate.cql3.handler.delegate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.util.Callback;

public interface ColumnHandlerDelegate {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void collectDeletionIdentifiers(Iterable<Object> columnValues, DeleteContext context);

    Object convertToInsertValue(Object facetValue, SaveContext saveContext);

    DataType getDataType();

    Object convertElement(Object parameterValue);

    void createValueConverter(Callback<ValueConverter> target, Iterable<Object> columnValues, QueryContext context);

    boolean isCascading();
}
