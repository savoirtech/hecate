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

import java.util.function.Function;

public abstract class ElementColumnType<F,C> implements ColumnType {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected final ElementHandler elementHandler;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ElementColumnType(ElementHandler elementHandler) {
        this.elementHandler = elementHandler;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract C convertParameterValueInternal(F facetValue);

    protected abstract DataType getDataTypeInternal(DataType elementType);

    protected abstract C getInsertValueInternal(Dehydrator dehydrator, F facetValue);
    
    protected abstract void setFacetValueInternal(Hydrator hydrator, Object pojo, Facet facet, C cassandraValue);

//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Object convertParameterValue(Object facetValue) {
        if(facetValue == null) {
            return null;
        }
        return convertParameterValueInternal((F)facetValue);
    }

    @Override
    public DataType getDataType() {
        return getDataTypeInternal(elementHandler.getDataType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getInsertValue(Dehydrator dehydrator, Object facetValue) {
        if (facetValue == null) {
            return null;
        }
        return getInsertValueInternal(dehydrator, (F) facetValue);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setFacetValue(Hydrator hydrator, Object pojo, Facet facet, Object cassandraValue) {
        if(cassandraValue == null) {
            facet.setValue(pojo, null);
        }
        setFacetValueInternal(hydrator, pojo, facet, (C)cassandraValue);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected Function<Object,Object> toParameterValue() {
        return elementHandler::getParameterValue;
    }
    protected Function<Object, Object> toInsertValue(Dehydrator dehydrator) {
        return element -> elementHandler.getInsertValue(element, dehydrator);
    }
}
