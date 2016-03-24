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

package com.savoirtech.hecate.pojo.query.def;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.ParameterBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.query.ParameterConverter;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.query.PojoQueryBuilder;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.query.parameter.ConstantParameterConverter;
import com.savoirtech.hecate.pojo.query.parameter.ParameterBindingConverter;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public class DefaultPojoQueryBuilder<P> implements PojoQueryBuilder<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final PojoBinding<P> pojoBinding;
    private final Map<String,ParameterBinding> parameterBindings;
    private final PojoQueryContextFactory contextFactory;
    private final List<ParameterConverter> parameterConverters = new LinkedList<>();
    private final Select.Where select;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQueryBuilder(Session session, PojoBinding<P> pojoBinding, String tableName, PojoQueryContextFactory contextFactory) {
        this.session = session;
        this.pojoBinding = pojoBinding;
        this.parameterBindings = pojoBinding.getParameterBindings();
        this.contextFactory = contextFactory;
        this.select = pojoBinding.selectFrom(tableName);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoQueryBuilder Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoQueryBuilder<P> asc(String facetName) {
        select.orderBy(QueryBuilder.asc(binding(facetName).getColumnName()));
        return this;
    }

    @Override
    public PojoQuery<P> build() {
        return new DefaultPojoQuery<>(session, pojoBinding, contextFactory, select, parameterConverters);
    }

    @Override
    public PojoQueryBuilder<P> desc(String facetName) {
        select.orderBy(QueryBuilder.desc(binding(facetName).getColumnName()));
        return this;
    }

    @Override
    public PojoQueryBuilder<P> eq(String facetName) {
        return append(facetName, QueryBuilder::eq);
    }

    @Override
    public PojoQueryBuilder<P> eq(String facetName, Object value) {
        return append(facetName, QueryBuilder::eq, value);
    }

    @Override
    public PojoQueryBuilder<P> gt(String facetName) {
        return append(facetName, QueryBuilder::gt);
    }

    @Override
    public PojoQueryBuilder<P> gt(String facetName, Object value) {
        return append(facetName, QueryBuilder::gt, value);
    }

    @Override
    public PojoQueryBuilder<P> gte(String facetName) {
        return append(facetName, QueryBuilder::gte);
    }

    @Override
    public PojoQueryBuilder<P> gte(String facetName, Object value) {
        return append(facetName, QueryBuilder::gte, value);
    }

    @Override
    public PojoQueryBuilder<P> in(String facetName) {
        return append(facetName, QueryBuilder::in);
    }

    @Override
    public PojoQueryBuilder<P> in(String facetName, Object value) {
        return append(facetName, QueryBuilder::in, value);
    }

    @Override
    public PojoQueryBuilder<P> lt(String facetName) {
        return append(facetName, QueryBuilder::lt);
    }

    @Override
    public PojoQueryBuilder<P> lt(String facetName, Object value) {
        return append(facetName, QueryBuilder::lt, value);
    }

    @Override
    public PojoQueryBuilder<P> lte(String facetName) {
        return append(facetName, QueryBuilder::lte);
    }

    @Override
    public PojoQueryBuilder<P> lte(String facetName, Object value) {
        return append(facetName, QueryBuilder::lte);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private PojoQueryBuilder<P> append(String facetName, BiFunction<String,Object,Clause> clause) {
        return append(facetName, clause, ParameterBindingConverter::new);
    }

    private PojoQueryBuilder<P> append(String facetName, BiFunction<String,Object,Clause> clauseFn, Function<ParameterBinding,ParameterConverter> converterFn) {
        ParameterBinding binding = binding(facetName);
        select.and(clauseFn.apply(binding.getColumnName(), bindMarker()));
        parameterConverters.add(converterFn.apply(binding));
        return this;
    }

    private ParameterBinding binding(String facetName) {
        ParameterBinding binding = parameterBindings.get(facetName);
        if(binding == null) {
            throw new HecateException("Facet \"%s\" not found.", facetName);
        }
        return binding;
    }

    private PojoQueryBuilder<P> append(String facetName, BiFunction<String,Object,Clause> clause, Object value) {
        return append(facetName, clause, binding -> new ConstantParameterConverter(value));
    }
}
