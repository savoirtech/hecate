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
import com.savoirtech.hecate.pojo.mapping.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.ScalarFacetMapping;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoQueryBuilder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

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
    private String name;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    static Select.Where createSelect(PojoMapping<?> pojoMapping) {
        Select.Selection select = select();
        pojoMapping.getIdMappings().forEach(mapping -> select.column(mapping.getColumnName()));
        pojoMapping.getSimpleMappings().forEach(mapping -> select.column(mapping.getColumnName()));
        return select.from(pojoMapping.getTableName()).where();
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQueryBuilder(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        this.pojoMapping = pojoMapping;
        this.persistenceContext = persistenceContext;
        this.where = createSelect(pojoMapping);
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

    @Override
    public DefaultPojoQuery<P> build() {
        if (name == null) {
            return new DefaultPojoQuery<>(where.getQueryString(), persistenceContext, pojoMapping, where, parameterMappings, injectedParameters);
        }
        return new DefaultPojoQuery<>(name, persistenceContext, pojoMapping, where, parameterMappings, injectedParameters);
    }

    @Override
    public DefaultPojoQueryBuilder<P> desc(String facetName) {
        where.orderBy(QueryBuilder.desc(lookupColumn(facetName)));
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> eq(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.eq(mapping.getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> eq(String facetName, Object value) {
        FacetMapping mapping = lookupMapping(facetName);
        String columnName = mapping.getColumnName();
        where.and(QueryBuilder.eq(columnName, QueryBuilder.bindMarker()));
        injectParameter(mapping, value);
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> gt(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.gt(mapping.getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> gt(String facetName, Object value) {
        FacetMapping mapping = lookupMapping(facetName);
        String columnName = mapping.getColumnName();
        where.and(QueryBuilder.gt(columnName, QueryBuilder.bindMarker()));
        injectParameter(mapping, value);
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> gte(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.gte(mapping.getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> gte(String facetName, Object value) {
        FacetMapping mapping = lookupMapping(facetName);
        String columnName = mapping.getColumnName();
        where.and(QueryBuilder.gte(columnName, QueryBuilder.bindMarker()));
        injectParameter(mapping, value);
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> identifierEquals() {
        return eq(identifierFacetName());
    }

    @Override
    public DefaultPojoQueryBuilder<P> identifierIn() {
        return in(identifierFacetName());
    }

    @Override
    public DefaultPojoQueryBuilder<P> in(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.in(mapping.getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> in(String facetName, Object value) {
        FacetMapping mapping = lookupMapping(facetName);
        String columnName = mapping.getColumnName();
        where.and(QueryBuilder.in(columnName, QueryBuilder.bindMarker()));
        injectParameter(mapping, value);
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> lt(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.lt(mapping.getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> lt(String facetName, Object value) {
        FacetMapping mapping = lookupMapping(facetName);
        String columnName = mapping.getColumnName();
        where.and(QueryBuilder.lt(columnName, QueryBuilder.bindMarker()));
        injectParameter(mapping, value);
        return this;
    }

    @Override
    public DefaultPojoQueryBuilder<P> lte(String facetName) {
        FacetMapping mapping = lookupMapping(facetName);
        where.and(QueryBuilder.lte(mapping.getColumnName(), QueryBuilder.bindMarker()));
        parameterMappings.add(mapping);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> lte(String facetName, Object value) {
        FacetMapping mapping = lookupMapping(facetName);
        String columnName = mapping.getColumnName();
        where.and(QueryBuilder.lte(columnName, QueryBuilder.bindMarker()));
        injectParameter(mapping, value);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> withName(String name) {
        this.name = name;
        return this;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private String identifierFacetName() {
        List<ScalarFacetMapping> idMappings = pojoMapping.getIdMappings();
        if (idMappings.size() > 1) {
            throw new HecateException("Composite primary keys not supported.");
        }
        return idMappings.get(0).getFacet().getName();
    }

    private void injectParameter(FacetMapping facetMapping, Object value) {
        Object columnValue = facetMapping.getColumnValueForFacetValue(value);
        injectedParameters.add(new InjectedParameter(injectedParameters.size() + parameterMappings.size(), columnValue));
    }

    private String lookupColumn(String facetName) {
        return lookupMapping(facetName).getColumnName();
    }

    private FacetMapping lookupMapping(String facetName) {
        FacetMapping mapping = facetMappings.get(facetName);
        if (mapping == null) {
            throw new HecateException("No facet named %s found in class %s.", facetName, pojoMapping.getPojoClass().getSimpleName());
        }
        return mapping;
    }
}
