package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;
import com.savoirtech.hecate.cql3.util.InjectionTarget;

import java.util.Map;

public abstract class AbstractColumnHandler implements ColumnHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnHandlerDelegate delegate;
    private final DataType columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected AbstractColumnHandler(ColumnHandlerDelegate delegate, DataType columnType) {
        this.delegate = delegate;
        this.columnType = columnType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object convertToFacetValue(Object columnValue, Map<Object, Object> conversions);

    protected abstract Iterable<Object> toColumnValues(Object columnValue);

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getColumnType() {
        return columnType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void getDeletionIdentifiers(Object columnValue, DeleteContext context) {
        if (columnValue != null) {
            getDelegate().collectDeletionIdentifiers(toColumnValues(columnValue), context);
        }
    }

    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return parameterValue == null ? null : getDelegate().getWhereClauseValue(parameterValue);
    }

    @Override
    public void injectFacetValue(InjectionTarget<Object> target, Object columnValue, QueryContext context) {
        if (columnValue != null) {
            getDelegate().injectFacetValues(new InjectionTargetWrapper(target, columnValue), toColumnValues(columnValue), context);
        }
    }

    @Override
    public boolean isCascading() {
        return getDelegate().isCascading();
    }

    protected ColumnHandlerDelegate getDelegate() {
        return delegate;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class InjectionTargetWrapper implements InjectionTarget<Map<Object, Object>> {
        private final InjectionTarget<Object> originalTarget;
        private final Object originalValue;

        private InjectionTargetWrapper(InjectionTarget<Object> originalTarget, Object originalValue) {
            this.originalTarget = originalTarget;
            this.originalValue = originalValue;
        }

        @Override
        public void inject(Map<Object, Object> value) {
            originalTarget.inject(convertToFacetValue(originalValue, value));
        }
    }
}
