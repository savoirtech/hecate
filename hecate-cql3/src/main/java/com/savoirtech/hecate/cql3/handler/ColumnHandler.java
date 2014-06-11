package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.util.Callback;

public interface ColumnHandler<C, F> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    DataType getColumnType();

    void getDeletionIdentifiers(C columnValue, DeleteContext context);

    void injectFacetValue(Callback<F> target, C columnValue, QueryContext context);

    C getInsertValue(F facetValue, SaveContext context);

    Object convertElement(Object parameterValue);

    boolean isCascading();
}
