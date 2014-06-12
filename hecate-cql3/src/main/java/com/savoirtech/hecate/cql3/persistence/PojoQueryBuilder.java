/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;

import java.util.LinkedList;
import java.util.List;

public class PojoQueryBuilder {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoMapping mapping;
    private final Select.Where where;
    private final List<FacetMapping> parameterMappings = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoQueryBuilder(PojoMapping mapping) {
        this.mapping = mapping;
        this.where = selectStub(mapping);
    }

    private static Select.Where selectStub(PojoMapping mapping) {
        final Select.Selection select = QueryBuilder.select();
        for (FacetMapping facetMapping : mapping.getFacetMappings()) {
            select.column(facetMapping.getFacetMetadata().getColumnName());
        }
        return select.from(mapping.getTableName()).where();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public PojoQueryBuilder eq(String facetName) {
        return eq(facetName, QueryBuilder.bindMarker());
    }

    public PojoQueryBuilder eq(String facetName, Object value) {
        where.and(QueryBuilder.eq(lookupColumn(facetName), value));
        return this;
    }

    private String lookupColumn(String facetName) {
        for (FacetMapping facetMapping : mapping.getFacetMappings()) {
            if (facetName.equals(facetMapping.getFacetMetadata().getColumnName())) {
                parameterMappings.add(facetMapping);
                return facetMapping.getFacetMetadata().getColumnName();
            }
        }
        throw new IllegalArgumentException(String.format("Facet %s not found on object of type %s.", facetName, mapping.getPojoMetadata().getPojoType().getName()));
    }

    public PojoQueryBuilder gt(String facetName) {
        return gt(facetName, QueryBuilder.bindMarker());
    }

    public PojoQueryBuilder gt(String facetName, Object value) {
        where.and(QueryBuilder.gt(lookupColumn(facetName), value));
        return this;
    }

    public PojoQueryBuilder gte(String facetName) {
        return gte(facetName, QueryBuilder.bindMarker());
    }

    public PojoQueryBuilder gte(String facetName, Object value) {
        where.and(QueryBuilder.gte(lookupColumn(facetName), value));
        return this;
    }

    public PojoQueryBuilder in(String facetName) {
        return in(facetName, QueryBuilder.bindMarker());
    }

    public PojoQueryBuilder in(String facetName, Object value) {
        where.and(QueryBuilder.in(lookupColumn(facetName), value));
        return this;
    }

    public PojoQueryBuilder lt(String facetName) {
        return lt(facetName, QueryBuilder.bindMarker());
    }

    public PojoQueryBuilder lt(String facetName, Object value) {
        where.and(QueryBuilder.lt(lookupColumn(facetName), value));
        return this;
    }

    public PojoQueryBuilder lte(String facetName) {
        return lte(facetName, QueryBuilder.bindMarker());
    }

    public PojoQueryBuilder lte(String facetName, Object value) {
        where.and(QueryBuilder.lte(lookupColumn(facetName), value));
        return this;
    }
}
