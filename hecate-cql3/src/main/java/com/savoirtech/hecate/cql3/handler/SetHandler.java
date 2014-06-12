/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;

import java.util.HashSet;
import java.util.Set;

public class SetHandler extends AbstractColumnHandler<Set<Object>, Set<Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SetHandler(ColumnHandlerDelegate delegate) {
        super(delegate, DataType.set(delegate.getDataType()));
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Set<Object> getInsertValue(Set<Object> facetValue, Dehydrator dehydrator) {
        if (facetValue == null) {
            return null;
        }
        return copyFacetValues(facetValue, new HashSet<>(), dehydrator);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Set<Object> convertToFacetValue(Set<Object> columnValue, ValueConverter converter) {
        if (columnValue == null) {
            return null;
        }
        return copyColumnValues(columnValue, new HashSet<>(), converter);
    }

    @Override
    protected Iterable<Object> toColumnValues(Set<Object> columnValue) {
        return columnValue;
    }
}
