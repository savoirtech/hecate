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

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;

import java.util.Arrays;

public class SimpleHandler extends AbstractColumnHandler<Object, Object> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SimpleHandler(ColumnHandlerDelegate delegate) {
        super(delegate, delegate.getDataType());
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Object getInsertValue(Object facetValue, SaveContext context) {
        return facetValue == null ? null : getDelegate().convertToInsertValue(facetValue, context);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object convertToFacetValue(Object columnValue, ValueConverter converter) {
        return converter.fromCassandraValue(columnValue);
    }

    @Override
    protected Iterable<Object> toColumnValues(Object columnValue) {
        return Arrays.asList(columnValue);
    }
}
