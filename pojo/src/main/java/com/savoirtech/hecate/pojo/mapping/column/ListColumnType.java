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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ListColumnType implements ColumnType<List<Object>,List<Object>> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Iterable<Object> columnElements(List<Object> columnValue) {
        return columnValue;
    }

    @Override
    public Iterable<Object> facetElements(List<Object> facetValue) {
        return facetValue;
    }

    @Override
    public List<Object> getColumnValue(List<Object> facetValue, Function<Object, Object> function) {
        return facetValue.stream().map(function).collect(Collectors.toList());
    }

    @Override
    public DataType getDataType(DataType elementDataType) {
        return DataType.list(elementDataType);
    }

    @Override
    public List<Object> getFacetValue(List<Object> columnValue, Function<Object, Object> function, Class<?> elementType) {
        return columnValue.stream().map(function).filter(val -> val != null).collect(Collectors.toList());
    }
}
