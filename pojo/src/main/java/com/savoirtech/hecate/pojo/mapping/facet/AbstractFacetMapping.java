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

package com.savoirtech.hecate.pojo.mapping.facet;

import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.mapping.column.ColumnType;

import java.util.function.Function;

public abstract class AbstractFacetMapping implements FacetMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Facet facet;
    private final ColumnType<Object,Object> columnType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected AbstractFacetMapping(Facet facet, ColumnType<Object, Object> columnType) {
        this.facet = facet;
        this.columnType = columnType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Function<Object,Object> elementColumnValue();

//----------------------------------------------------------------------------------------------------------------------
// FacetMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object getColumnValue(Object pojo) {
        final Object facetValue = facetValue(pojo);
        return getColumnValueForFacetValue(facetValue);
    }

    @Override
    public Object getColumnValueForFacetValue(Object facetValue) {
        return facetValue == null ? null : columnType.getColumnValue(facetValue, elementColumnValue());
    }

    @Override
    public Facet getFacet() {
        return facet;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public ColumnType<Object, Object> getColumnType() {
        return columnType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        return getFacet().getName() + " @ " + getFacet().getColumnName();
    }

    protected Object facetValue(Object pojo) {
        return facet.getValue(pojo);
    }
}
