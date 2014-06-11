/*
 * Copyright (c) 2014. Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.handler;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;
import com.savoirtech.hecate.cql3.util.Callback;

import java.util.Collection;

public abstract class AbstractColumnHandler<C, F> implements ColumnHandler<C, F> {
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

    protected abstract F convertToFacetValue(C columnValue, ValueConverter converter);

    protected abstract Iterable<Object> toColumnValues(C columnValue);

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object convertElement(Object parameterValue) {
        return parameterValue == null ? null : getDelegate().convertElement(parameterValue);
    }

    @Override
    public DataType getColumnType() {
        return columnType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void getDeletionIdentifiers(C columnValue, DeleteContext context) {
        if (columnValue != null) {
            getDelegate().collectDeletionIdentifiers(toColumnValues(columnValue), context);
        }
    }

    @Override
    public void injectFacetValue(Callback<F> target, C columnValue, QueryContext context) {
        if (columnValue != null) {
            getDelegate().createValueConverter(new ValueConverterCallback(target, columnValue), toColumnValues(columnValue), context);
        }
    }

    @Override
    public boolean isCascading() {
        return getDelegate().isCascading();
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    protected ColumnHandlerDelegate getDelegate() {
        return delegate;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected <T extends Collection<Object>> T copyColumnValues(Collection<Object> columnValues, T facetValues, ValueConverter converter) {
        for (Object value : columnValues) {
            facetValues.add(converter.fromCassandraValue(value));
        }
        return facetValues;
    }

    protected <T extends Collection<Object>> T copyFacetValues(Collection<Object> facetValues, T columnValues, SaveContext context) {
        for (Object value : facetValues) {
            columnValues.add(getDelegate().convertToInsertValue(value, context));
        }
        return columnValues;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class ValueConverterCallback implements Callback<ValueConverter> {
        private final Callback<F> originalTarget;
        private final C originalValue;

        private ValueConverterCallback(Callback<F> originalTarget, C originalValue) {
            this.originalTarget = originalTarget;
            this.originalValue = originalValue;
        }

        @Override
        public void execute(ValueConverter valueConverter) {
            originalTarget.execute(convertToFacetValue(originalValue, valueConverter));
        }
    }
}
