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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.ParameterBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.query.ParameterConverter;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.query.PojoQueryBuilder;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.query.parameter.ConstantParameterConverter;
import com.savoirtech.hecate.pojo.query.parameter.ParameterBindingConverter;

public class DefaultPojoQueryBuilder<P> implements PojoQueryBuilder<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final CqlSession session;
    private final PojoBinding<P> pojoBinding;
    private final Map<String,ParameterBinding> parameterBindings;
    private final PojoQueryContextFactory contextFactory;
    private final List<ParameterConverter> parameterConverters = new LinkedList<>();
    private Select select;
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static List<Object> convertToList(ParameterBinding binding, Iterable<?> params) {
        return StreamSupport.stream(params.spliterator(), false).map(binding::toColumnValue).collect(Collectors.toList());
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQueryBuilder(CqlSession session, PojoBinding<P> pojoBinding, String tableName, PojoQueryContextFactory contextFactory, Executor executor) {
        this.session = session;
        this.pojoBinding = pojoBinding;
        this.parameterBindings = pojoBinding.getParameterBindings();
        this.contextFactory = contextFactory;
        this.select = pojoBinding.selectFrom(tableName);
        this.executor = executor;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoQueryBuilder Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoQueryBuilder<P> asc(String facetName) {
        this.select = select.orderBy(binding(facetName).getColumnName(), ClusteringOrder.ASC);
        return this;
    }

    @Override
    public PojoQuery<P> build() {
        return new DefaultPojoQuery<>(session, pojoBinding, contextFactory, select, parameterConverters, executor);
    }

    @Override
    public PojoQueryBuilder<P> desc(String facetName) {
        this.select = select.orderBy(binding(facetName).getColumnName(), ClusteringOrder.DESC);
        return this;
    }

    @Override
    public PojoQueryBuilder<P> eq(String facetName) {
        return append(facetName, builder -> builder.isEqualTo(bindMarker()));
    }

    @Override
    public PojoQueryBuilder<P> eq(String facetName, Object value) {
        return append(facetName, builder -> builder.isEqualTo(bindMarker()), value);
    }

    @Override
    public PojoQueryBuilder<P> gt(String facetName) {
        return append(facetName, builder -> builder.isGreaterThan(bindMarker()));
    }

    @Override
    public PojoQueryBuilder<P> gt(String facetName, Object value) {
        return append(facetName, builder -> builder.isGreaterThan(bindMarker()), value);
    }

    @Override
    public PojoQueryBuilder<P> gte(String facetName) {
        return append(facetName, builder -> builder.isGreaterThanOrEqualTo(bindMarker()));
    }

    @Override
    public PojoQueryBuilder<P> gte(String facetName, Object value) {
        return append(facetName, builder -> builder.isGreaterThanOrEqualTo(bindMarker()), value);
    }

    @Override
    public PojoQueryBuilder<P> in(String facetName) {
        return append(facetName, builder -> builder.in(bindMarker()), InConverter::new);
    }

    @Override
    public PojoQueryBuilder<P> in(String facetName, Iterable<Object> values) {
        return append(facetName, builder -> builder.in(bindMarker()), binding -> new ConstantParameterConverter(convertToList(binding, values)));
    }

    @Override
    public PojoQueryBuilder<P> lt(String facetName) {
        return append(facetName, builder -> builder.isLessThan(bindMarker()));
    }

    @Override
    public PojoQueryBuilder<P> lt(String facetName, Object value) {
        return append(facetName, builder -> builder.isLessThan(bindMarker()), value);
    }

    @Override
    public PojoQueryBuilder<P> lte(String facetName) {
        return append(facetName, builder -> builder.isLessThanOrEqualTo(bindMarker()));
    }

    @Override
    public PojoQueryBuilder<P> lte(String facetName, Object value) {
        return append(facetName, builder -> builder.isLessThanOrEqualTo(bindMarker()), value);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private PojoQueryBuilder<P> append(String facetName, Function<ColumnRelationBuilder<Select>, Select> columnFunction) {
        return append(facetName, columnFunction, ParameterBindingConverter::new);
    }

    private PojoQueryBuilder<P> append(String facetName, Function<ColumnRelationBuilder<Select>, Select> columnFunction, Function<ParameterBinding,ParameterConverter> converterFn) {
        ParameterBinding binding = binding(facetName);
        this.select = columnFunction.apply(select.whereColumn(binding.getColumnName()));
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

    private PojoQueryBuilder<P> append(String facetName, Function<ColumnRelationBuilder<Select>, Select> columnFunction, Object value) {
        return append(facetName, columnFunction, binding -> new ConstantParameterConverter(binding(facetName).toColumnValue(value)));
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class InConverter implements ParameterConverter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final ParameterBinding parameterBinding;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public InConverter(ParameterBinding parameterBinding) {
            this.parameterBinding = parameterBinding;
        }

//----------------------------------------------------------------------------------------------------------------------
// ParameterConverter Implementation
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public Object convertParameter(Iterator<Object> parameters) {
            Object param = parameters.next();
            if(param instanceof Iterable) {
                return convertToList(parameterBinding, (Iterable<?>) param);
            }
            throw new HecateException("Invalid parameter type (%s) for IN expression, %s<Object> required.", param.getClass().getCanonicalName(), Iterable.class.getCanonicalName());
        }
    }
}
