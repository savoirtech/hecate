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
import com.savoirtech.hecate.pojo.mapping.element.ElementHandler;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ArrayColumnType extends ElementColumnType<Object> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayColumnType(ElementHandler elementHandler) {
        super(elementHandler);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected DataType getDataTypeInternal(DataType elementType) {
        return DataType.list(elementType);
    }

    @Override
    protected Object getInsertValueInternal(Dehydrator dehydrator, Object array) {
        final int length = Array.getLength(array);
        final List<Object> columnValues = new ArrayList<>(length);
        Function<Object, Object> toInsertValue = toInsertValue(dehydrator);
        for (int i = 0; i < length; ++i) {
            final Object value = Array.get(array, i);
            columnValues.add(toInsertValue.apply(value));
        }
        return columnValues;
    }
}
