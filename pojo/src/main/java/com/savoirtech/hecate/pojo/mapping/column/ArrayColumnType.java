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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ArrayColumnType implements ColumnType<List<Object>, Object> {
//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Iterable<Object> columnElements(List<Object> columnValue) {
        return columnValue;
    }

    @Override
    public Iterable<Object> facetElements(Object array) {
        return arrayToList(array, element -> element);
    }

    @Override
    public List<Object> getColumnValue(Object array, Function<Object, Object> function) {
        return arrayToList(array, function);
    }

    @Override
    public DataType getDataType(DataType elementDataType) {
        return DataType.list(elementDataType);
    }

    @Override
    public Object getFacetValue(List<Object> columnValue, Function<Object, Object> function, Class<?> elementType) {
        Object array = Array.newInstance(elementType, columnValue.size());
        int index = 0;
        for (Object element : columnValue) {
            Array.set(array, index, function.apply(element));
            index++;
        }
        return array;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private List<Object> arrayToList(Object array, Function<Object, Object> elementConverter) {
        final int length = Array.getLength(array);
        final List<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final Object elementValue = Array.get(array, i);
            list.add(elementConverter.apply(elementValue));
        }
        return list;
    }
}
