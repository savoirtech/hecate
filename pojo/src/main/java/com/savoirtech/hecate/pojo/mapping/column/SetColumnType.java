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

import java.util.Set;
import java.util.stream.Collectors;

public class SetColumnType extends ElementColumnType<Set<Object>,Set<Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SetColumnType(ElementHandler elementHandler) {
        super(elementHandler);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Set<Object> convertParameterValueInternal(Set<Object> facetValue) {
        return facetValue.stream().map(toParameterValue()).collect(Collectors.toSet());
    }

    @Override
    protected DataType getDataTypeInternal(DataType elementType) {
        return DataType.set(elementType);
    }

    @Override
    protected Set<Object> getInsertValueInternal(Dehydrator dehydrator, Set<Object> facetValue) {
        return facetValue.stream().map(toInsertValue(dehydrator)).collect(Collectors.toSet());
    }

    @Override
    protected void setFacetValueInternal(Hydrator hydrator, Object pojo, Facet facet, Set<Object> cassandraValues) {
        elementHandler.resolveElements(cassandraValues, hydrator, resolver -> {
            final Set<Object> facetValues = cassandraValues.stream().map(resolver::resolveElement).collect(Collectors.toSet());
            facet.setValue(pojo, facetValues);
        });
    }
}
