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

package com.savoirtech.hecate.pojo.persistence.def;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.mapping.facet.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.facet.ScalarFacetMapping;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoQueryBuilder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DefaultPojoQueryBuilder<P> implements PojoQueryBuilder<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final PojoMapping<P> pojoMapping;
    private final Map<String, FacetMapping> facetMappings;
    private final Select.Where where;
    private final List<FacetMapping> parameterMappings = new LinkedList<>();
    private final List<InjectedParameter> injectedParameters = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQueryBuilder(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        this.pojoMapping = pojoMapping;
        this.persistenceContext = persistenceContext;
        this.where = pojoMapping.createSelectStatement();
        this.facetMappings = toFacetMappingsMap(pojoMapping);
    }

    private static Map<String, FacetMapping> toFacetMappingsMap(PojoMapping<?> pojoMapping) {
        Map<String, FacetMapping> mappings = new HashMap<>();
        pojoMapping.getIdMappings().forEach(mapping -> mappings.put(mapping.getFacet().getName(), mapping));
        pojoMapping.getSimpleMappings().forEach(mapping -> mappings.put(mapping.getFacet().getName(), mapping));
        return mappings;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoQueryBuilder Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DefaultPojoQueryBuilder<P> asc(String facetName) {
        where.orderBy(QueryBuilder.asc(lookupColumn(facetName)));
        return this;
    }

    //----------------------------------------------------------------------------------------------------------------------
    // PojoQueryBuilder Implementation
    //----------------------------------------------------------------------------------------------------------------------

    @Override
    public DefaultPojoQuery<P> build() {
        return new DefaultPojoQuery<>(persistenceContext, pojoMapping, where, parameterMappings, injectedParameters);
    }

    @Override
    public DefaultPojoQueryBuilder<P> desc(String facetName) {
        where.orderBy(QueryBuilder.desc(lookupColumn(facetName)));
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> eq(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.eq(mapping.getFacet().getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> eq(String facetName, Object value) {
        injectedParameters.add(injected(facetName, value));
        return eq(facetName);
    }

    @Override
    public DefaultPojoQueryBuilder<P> gt(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.gt(mapping.getFacet().getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> gt(String facetName, Object value) {
        injectedParameters.add(injected(facetName, value));
        return gt(facetName);
    }

    @Override
    public DefaultPojoQueryBuilder<P> gte(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.gte(mapping.getFacet().getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> gte(String facetName, Object value) {
        injectedParameters.add(injected(facetName, value));
        return gte(facetName);
    }

    @Override
    public DefaultPojoQueryBuilder<P> identifierEquals() {
        return eq(identifierColumn());
    }

    @Override
    public DefaultPojoQueryBuilder<P> identifierIn() {
        return in(identifierColumn());
    }

    @Override
    public DefaultPojoQueryBuilder<P> in(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.in(mapping.getFacet().getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> in(String facetName, Object value) {
        injectedParameters.add(injected(facetName, value));
        return in(facetName);
    }

    @Override
    public DefaultPojoQueryBuilder<P> lt(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.lt(mapping.getFacet().getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> lt(String facetName, Object value) {
        injectedParameters.add(injected(facetName, value));
        return lt(facetName);
    }

    @Override
    public DefaultPojoQueryBuilder<P> lte(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.lte(mapping.getFacet().getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> lte(String facetName, Object value) {
        injectedParameters.add(injected(facetName, value));
        return lte(facetName);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private String identifierColumn() {
        List<ScalarFacetMapping> idMappings = pojoMapping.getIdMappings();
        if (idMappings.size() > 1) {
            throw new HecateException("Composite primary keys not supported.");
        }
        return idMappings.get(0).getFacet().getColumnName();
    }

    private InjectedParameter injected(String facetName, Object value) {
        FacetMapping facetMapping = lookupMapping(facetName);
        Object cassandraValue = facetMapping.getColumnValueForFacetValue(value);
        return new InjectedParameter(parameterMappings.size(), cassandraValue);
    }

    private FacetMapping lookupMapping(String facetName) {
        FacetMapping mapping = facetMappings.get(facetName);
        if (mapping == null) {
            throw new HecateException("No facet named %s found in class %s.", facetName, pojoMapping.getPojoClass().getSimpleName());
        }
        return mapping;
    }

    private String lookupColumn(String facetName) {
        return lookupMapping(facetName).getFacet().getColumnName();
    }
}
