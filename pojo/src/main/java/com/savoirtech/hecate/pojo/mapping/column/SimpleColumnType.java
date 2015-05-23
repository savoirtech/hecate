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

import java.util.Collections;
import java.util.function.Function;

public class SimpleColumnType implements ColumnType<Object,Object> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    public static final SimpleColumnType INSTANCE = new SimpleColumnType();

    @Override
    public Iterable<Object> columnElements(Object columnValue) {
        return columnValue == null ? Collections.emptyList() : Collections.singleton(columnValue);
    }

    @Override
    public Object getColumnValue(Object facetValue, Function<Object, Object> function) {
        return function.apply(facetValue);
    }

    @Override
    public DataType getDataType(DataType elementDataType) {
        return elementDataType;
    }

    @Override
    public Object getFacetValue(Object columnValue, Function<Object, Object> function, Class<?> elementType) {
        return function.apply(columnValue);
    }

    @Override
    public Iterable<Object> facetElements(Object facetValue) {
        return facetValue == null ? Collections.emptyList() : Collections.singleton(facetValue);
    }
}
