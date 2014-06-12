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

package com.savoirtech.hecate.cql3.persistence.def;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.PojoQueryBuilder;

import java.util.LinkedList;
import java.util.List;

public class DefaultPojoQueryBuilder<P> implements PojoQueryBuilder<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final DefaultPersistenceContext persistenceContext;
    private final PojoMapping mapping;
    private final Select.Where where;
    private final List<FacetMapping> parameterMappings = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQueryBuilder(DefaultPersistenceContext persistenceContext, PojoMapping mapping) {
        this.mapping = mapping;
        this.persistenceContext = persistenceContext;
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
// PojoQueryBuilder Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DefaultPojoQuery<P> build() {
        return new DefaultPojoQuery<>(persistenceContext, where, mapping, parameterMappings);
    }

    @Override
    public DefaultPojoQueryBuilder<P> eq(String facetName) {
        where.and(QueryBuilder.eq(lookupColumn(facetName), QueryBuilder.bindMarker()));
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> gt(String facetName) {
        where.and(QueryBuilder.gt(lookupColumn(facetName), QueryBuilder.bindMarker()));
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> gte(String facetName) {
        where.and(QueryBuilder.gte(lookupColumn(facetName), QueryBuilder.bindMarker()));
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> identifierEquals() {
        return eq(identifierName());
    }

    @Override
    public DefaultPojoQueryBuilder<P> identifierIn() {
        return in(identifierName());
    }

    @Override
    public DefaultPojoQueryBuilder<P> in(String facetName) {
        where.and(QueryBuilder.in(lookupColumn(facetName), QueryBuilder.bindMarker()));
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> lt(String facetName) {
        QueryBuilder.lt(lookupColumn(facetName), QueryBuilder.bindMarker());
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> lte(String facetName) {
        where.and(QueryBuilder.lte(lookupColumn(facetName), QueryBuilder.bindMarker()));
        return this;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private String identifierName() {
        return mapping.getIdentifierMapping().getFacetMetadata().getFacet().getName();
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
}
