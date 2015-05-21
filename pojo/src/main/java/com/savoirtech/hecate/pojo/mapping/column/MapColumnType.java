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
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.mapping.element.ElementHandler;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.Hydrator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class MapColumnType extends ElementColumnType<Map<Object, Object>, Map<Object, Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Converter keyConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public MapColumnType(Converter keyConverter, ElementHandler elementHandler) {
        super(elementHandler);
        this.keyConverter = keyConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected DataType getDataTypeInternal(DataType elementType) {
        return DataType.map(keyConverter.getDataType(), elementType);
    }

    @Override
    protected Map<Object, Object> getInsertValueInternal(Dehydrator dehydrator, Map<Object, Object> facetValue) {
        Function<Object, Object> elementConverter = toInsertValue(dehydrator);
        return convertMap(facetValue, elementConverter);
    }

    private Map<Object, Object> convertMap(Map<Object, Object> facetValue, Function<Object, Object> elementConverter) {
        Map<Object, Object> value = new HashMap<>();
        for (Map.Entry<Object, Object> entry : facetValue.entrySet()) {
            value.put(keyConverter.toCassandraValue(entry.getKey()), elementConverter.apply(entry.getValue()));
        }
        return value;
    }

    @Override
    protected Map<Object, Object> convertParameterValueInternal(Map<Object, Object> facetValue) {
        return convertMap(facetValue, toParameterValue());
    }

    @Override
    protected void setFacetValueInternal(Hydrator hydrator, Object pojo, Facet facet, Map<Object, Object> cassandraValue) {
        elementHandler.resolveElements(cassandraValue.values(), hydrator, resolver -> {
            Map<Object, Object> facetValue = new HashMap<>();
            for (Map.Entry<Object, Object> entry : cassandraValue.entrySet()) {
                final Object value = resolver.resolveElement(entry.getValue());
                if (value != null) {
                    facetValue.put(keyConverter.fromCassandraValue(entry.getKey()), value);
                }
            }
            facet.setValue(pojo, facetValue);
        });
    }
}
