package com.savoirtech.hecate.cql3.handler.delegate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.util.InjectionTarget;

import java.util.HashMap;
import java.util.Map;

public class ScalarDelegate implements ColumnHandlerDelegate {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter valueConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarDelegate(ValueConverter valueConverter) {
        this.valueConverter = valueConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandlerDelegate Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object convertToInsertValue(Object facetValue, SaveContext saveContext) {
        return valueConverter.toCassandraValue(facetValue);
    }

    @Override
    public DataType getDataType() {
        return valueConverter.getDataType();
    }

    @Override
    public void collectDeletionIdentifiers(Iterable<Object> columnValues, DeleteContext context) {
        // Do nothing!
    }

    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return valueConverter.toCassandraValue(parameterValue);
    }

    @Override
    public void injectFacetValues(InjectionTarget<Map<Object, Object>> target, Iterable<Object> columnValues, QueryContext context) {
        Map<Object, Object> converted = new HashMap<>();
        for (Object columnValue : columnValues) {
            converted.put(columnValue, valueConverter.fromCassandraValue(columnValue));
        }
        target.inject(converted);
    }

    @Override
    public boolean isCascading() {
        return false;
    }
}
