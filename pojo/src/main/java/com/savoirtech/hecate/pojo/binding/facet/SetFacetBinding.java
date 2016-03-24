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
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.binding.ElementBinding;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import com.savoirtech.hecate.pojo.facet.Facet;

public class SetFacetBinding extends OneToManyFacetBinding<Set<Object>,Set<Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SetFacetBinding(Facet facet, String columnName, ElementBinding elementBinding) {
        super(facet, columnName, elementBinding);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Collection<Object> elementsOf(Set<Object> facetValue) {
        return facetValue;
    }

    @Override
    protected DataType getDataType() {
        return DataType.set(getElementBinding().getElementDataType());
    }

    @Override
    protected Set<Object> toColumnValue(Set<Object> facetValue) {
        return facetValue.stream().map(getElementBinding()::toColumnValue).collect(Collectors.toSet());
    }

    @Override
    protected Set<Object> toFacetValue(Set<Object> columnValue, PojoQueryContext context) {
        return columnValue.stream().map(element -> getElementBinding().toFacetValue(element, context)).collect(Collectors.toSet());
    }

}
