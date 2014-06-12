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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ArrayHandler extends AbstractColumnHandler<List<Object>, Object> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> elementType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayHandler(Class<?> elementType, ColumnHandlerDelegate delegate) {
        super(delegate, DataType.list(delegate.getDataType()));
        this.elementType = elementType;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<Object> getInsertValue(Object array, Dehydrator dehydrator) {
        if (array == null) {
            return null;
        }
        final int length = Array.getLength(array);
        final List<Object> columnValues = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            final Object value = Array.get(array, i);
            columnValues.add(getDelegate().convertToInsertValue(value, dehydrator));
        }
        return columnValues;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object convertToFacetValue(List<Object> columnValue, ValueConverter converter) {
        if (columnValue == null) {
            return null;
        }
        Object array = Array.newInstance(elementType, columnValue.size());
        int index = 0;
        for (Object element : columnValue) {
            Array.set(array, index, converter.fromCassandraValue(element));
            index++;
        }
        return array;
    }

    @Override
    protected Iterable<Object> toColumnValues(List<Object> columnValue) {
        return columnValue;
    }
}
