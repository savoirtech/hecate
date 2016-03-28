/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.binding.facet;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.ElementBinding;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public class ArrayFacetBinding<C,F> extends OneToManyFacetBinding<List<Object>,Object> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayFacetBinding(Facet facet, String columnName, ElementBinding elementBinding) {
        super(facet, columnName, elementBinding);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Collection<Object> elementsOf(Object facetValue) {
        int length = Array.getLength(facetValue);
        List<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(Array.get(facetValue, i));
        }
        return list;
    }

    @Override
    protected DataType getDataType() {
        return DataType.list(getElementBinding().getElementDataType());
    }

    @Override
    protected List<Object> toColumnValue(Object facetValue) {
        final int length = Array.getLength(facetValue);
        final List<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final Object elementValue = Array.get(facetValue, i);
            if(elementValue == null) {
                throw new HecateException("Cassandra driver does not support null values inside %s collections.", getDataType());
            }
            list.add(getElementBinding().toColumnValue(elementValue));
        }
        return list;
    }

    @Override
    protected Object toFacetValue(List<Object> columnValue, PojoQueryContext context) {
        Object array = Array.newInstance(getElementBinding().getElementType(), columnValue.size());
        int index = 0;
        for (Object element : columnValue) {
            Array.set(array, index++, getElementBinding().toFacetValue(element, context));
        }
        return array;
    }
}
