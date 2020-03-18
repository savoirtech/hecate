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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.ElementBinding;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public class ListFacetBinding extends OneToManyFacetBinding<List<Object>, List<Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ListFacetBinding(Facet facet, String columnName, ElementBinding elementBinding) {
        super(facet, columnName, elementBinding);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Collection<Object> elementsOf(List<Object> facetValue) {
        return facetValue;
    }

    @Override
    protected DataType getDataType() {
        return DataTypes.listOf(getElementBinding().getElementDataType());
    }

    @Override
    protected List<Object> toColumnValue(List<Object> facetValue) {
        return facetValue.stream().map(elementValue -> {
            if(elementValue == null) {
                throw new HecateException("Cassandra driver does not support null values inside %s collections.", getDataType());
            }
            return getElementBinding().toColumnValue(elementValue);
        }).collect(Collectors.toList());
    }

    @Override
    protected List<Object> toFacetValue(List<Object> columnValue, PojoQueryContext context) {
        return columnValue.stream().map(element -> getElementBinding().toFacetValue(element, context)).collect(Collectors.toList());
    }
}
