/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.mapping.column;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.convert.Converter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class MapColumnType implements ColumnType<Map<Object, Object>, Map<Object, Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Converter keyConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public MapColumnType(Converter keyConverter) {
        this.keyConverter = keyConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Iterable<Object> columnElements(Map<Object, Object> columnValue) {
        return columnValue.values();
    }

    @Override
    public Iterable<Object> facetElements(Map<Object, Object> facetValue) {
        return facetValue.values();
    }

    @Override
    public Map<Object, Object> getColumnValue(Map<Object, Object> facetValue, Function<Object, Object> elementConverter) {
        Map<Object, Object> columnValue = new HashMap<>();
        for (Map.Entry<Object, Object> entry : facetValue.entrySet()) {
            columnValue.put(keyConverter.toColumnValue(entry.getKey()), elementConverter.apply(entry.getValue()));
        }
        return columnValue;
    }

    @Override
    public DataType getDataType(DataType elementDataType) {
        return DataType.map(keyConverter.getDataType(), elementDataType);
    }

    @Override
    public Map<Object, Object> getFacetValue(Map<Object, Object> columnValue, Function<Object, Object> function, Class<?> elementType) {
        Map<Object, Object> facetValue = new HashMap<>();
        for (Map.Entry<Object, Object> entry : columnValue.entrySet()) {
            facetValue.put(keyConverter.toFacetValue(entry.getKey()), function.apply(entry.getValue()));
        }
        return facetValue;
    }
}
