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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.binding.ElementBinding;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;

public class MapFacetBinding extends OneToManyFacetBinding<Map<Object, Object>, Map<Object, Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Converter keyConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public MapFacetBinding(Facet facet, String columnName, Converter keyConverter, ElementBinding elementBinding) {
        super(facet, columnName, elementBinding);
        this.keyConverter = keyConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Collection<Object> elementsOf(Map<Object, Object> facetValue) {
        return facetValue.values();
    }

    @Override
    protected DataType getDataType() {
        return DataType.map(keyConverter.getDataType(), getElementBinding().getElementDataType());
    }

    @Override
    protected Map<Object, Object> toColumnValue(Map<Object, Object> facetValue) {
        Map<Object, Object> columnValue = new HashMap<>();
        for (Map.Entry<Object, Object> entry : facetValue.entrySet()) {
            Object key = keyConverter.toColumnValue(entry.getKey());
            Object value = getElementBinding().toColumnValue(entry.getValue());
            columnValue.put(key, value);
        }
        return columnValue;
    }

    @Override
    protected Map<Object, Object> toFacetValue(Map<Object, Object> columnValue, PojoQueryContext context) {
        Map<Object, Object> facetValue = new HashMap<>();
        for (Map.Entry<Object, Object> entry : columnValue.entrySet()) {
            Object key = keyConverter.toFacetValue(entry.getKey());
            Object value = getElementBinding().toFacetValue(entry.getValue(), context);
            facetValue.put(key, value);
        }
        return facetValue;
    }
}
