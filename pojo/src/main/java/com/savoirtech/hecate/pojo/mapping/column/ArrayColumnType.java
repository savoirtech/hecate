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
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.mapping.element.ElementHandler;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.Hydrator;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ArrayColumnType extends ElementColumnType<Object,List<Object>> {
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
    protected List<Object> convertParameterValueInternal(Object facetValue) {
        return convertArray(facetValue, toParameterValue());
    }

    @Override
    protected DataType getDataTypeInternal(DataType elementType) {
        return DataType.list(elementType);
    }

    @Override
    protected List<Object> getInsertValueInternal(Dehydrator dehydrator, Object array) {
        return convertArray(array, toInsertValue(dehydrator));
    }

    private List<Object> convertArray(Object array, Function<Object, Object> elementConverter) {
        final int length = Array.getLength(array);
        final List<Object> columnValues = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            final Object value = Array.get(array, i);
            columnValues.add(elementConverter.apply(value));
        }
        return columnValues;
    }

    @Override
    protected void setFacetValueInternal(Hydrator hydrator, Object pojo, Facet facet, List<Object> cassandraValues) {
        elementHandler.resolveElements(cassandraValues, hydrator, resolver -> {
            Object array = Array.newInstance(facet.getType().getArrayElementType().getRawType(),cassandraValues.size());
            int index = 0;
            for (Object cassandraValue : cassandraValues) {
                Array.set(array, index, resolver.resolveElement(cassandraValue));
            }
            facet.setValue(pojo, array);
        });
    }
}
