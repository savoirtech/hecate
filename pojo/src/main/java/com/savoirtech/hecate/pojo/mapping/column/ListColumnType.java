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

import java.util.List;
import java.util.stream.Collectors;

public class ListColumnType extends ElementColumnType<List<Object>,List<Object>> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ListColumnType(ElementHandler elementHandler) {
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
    protected List<Object> getInsertValueInternal(Dehydrator dehydrator, List<Object> facetValue) {
        return facetValue.stream().map(toInsertValue(dehydrator)).collect(Collectors.toList());
    }

    @Override
    protected List<Object> convertParameterValueInternal(List<Object> facetValue) {
        return facetValue.stream().map(toParameterValue()).collect(Collectors.toList());
    }

    @Override
    protected void setFacetValueInternal(Hydrator hydrator, Object pojo, Facet facet, final List<Object> cassandraValues) {
        elementHandler.resolveElements(cassandraValues, hydrator, resolver -> {
            final List<Object> facetValues = cassandraValues.stream().map(resolver::resolveElement).filter(val -> val != null).collect(Collectors.toList());
            facet.setValue(pojo, facetValues);
        });
    }
}
