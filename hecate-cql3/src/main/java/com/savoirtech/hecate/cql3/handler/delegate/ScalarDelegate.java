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

package com.savoirtech.hecate.cql3.handler.delegate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.util.Callback;

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
    public Object convertElement(Object parameterValue) {
        return valueConverter.toCassandraValue(parameterValue);
    }

    @Override
    public void createValueConverter(Callback<ValueConverter> target, Iterable<Object> columnValues, QueryContext context) {
        target.execute(valueConverter);
    }


    @Override
    public boolean isCascading() {
        return false;
    }
}
