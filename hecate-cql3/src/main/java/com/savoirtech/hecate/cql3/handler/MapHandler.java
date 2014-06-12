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

import java.util.HashMap;
import java.util.Map;

public class MapHandler extends AbstractColumnHandler<Map<Object, Object>, Map<Object, Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter keyConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public MapHandler(ValueConverter keyConverter, ColumnHandlerDelegate delegate) {
        super(delegate, DataType.map(keyConverter.getDataType(), delegate.getDataType()));
        this.keyConverter = keyConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Map<Object, Object> getInsertValue(Map<Object, Object> facetValue, Dehydrator dehydrator) {
        if (facetValue == null) {
            return null;
        }
        Map<Object, Object> columnValue = new HashMap<>();
        for (Map.Entry<Object, Object> entry : facetValue.entrySet()) {
            columnValue.put(keyConverter.toCassandraValue(entry.getKey()),
                    getDelegate().convertToInsertValue(entry.getValue(), dehydrator));
        }
        return columnValue;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Map<Object, Object> convertToFacetValue(Map<Object, Object> columnValue, ValueConverter converter) {
        if (columnValue == null) {
            return null;
        }
        Map<Object, Object> facetValues = new HashMap<>();
        for (Map.Entry<Object, Object> entry : columnValue.entrySet()) {
            facetValues.put(keyConverter.fromCassandraValue(entry.getKey()), converter.fromCassandraValue(entry.getValue()));
        }
        return facetValues;
    }

    @Override
    protected Iterable<Object> toColumnValues(Map<Object, Object> columnValue) {
        return columnValue.values();
    }
}
