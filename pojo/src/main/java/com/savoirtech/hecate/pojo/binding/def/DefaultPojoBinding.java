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

package com.savoirtech.hecate.pojo.binding.def;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.binding.*;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import com.savoirtech.hecate.pojo.reflect.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public class DefaultPojoBinding<P> implements PojoBinding<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPojoBinding.class);
    
    private final Class<P> pojoType;
    private KeyBinding keyBinding;
    private List<ColumnBinding> facetBindings = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoBinding(Class<P> pojoType) {
        this.pojoType = pojoType;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public BoundStatement bindInsert(PreparedStatement statement, P pojo, int ttl) {
        List<Object> parameters = new LinkedList<>();
        keyBinding.collectParameters(pojo, parameters);
        facetBindings.forEach(facetBinding -> facetBinding.collectParameters(pojo, parameters));
        parameters.add(ttl);
        return bind(statement, parameters);
    }
    
    @Override
    public BoundStatement bindWhereIdEquals(PreparedStatement statement, List<Object> keys) {
        return bind(statement, keyBinding.getKeyParameters(keys));
    }

    @Override
    public P createPojo() {
        return ReflectionUtils.newInstance(pojoType);
    }

    @Override
    public Create createTable(String tableName) {
        Create create = SchemaBuilder.createTable(tableName);
        keyBinding.create(create);
        facetBindings.forEach(binding -> binding.create(create));
        return create.ifNotExists();
    }

    @Override
    public Delete.Where deleteFrom(String tableName) {
        Delete.Where delete = QueryBuilder.delete().from(tableName).where();
        keyBinding.delete(delete);
        return delete;
    }

    @Override
    public KeyBinding getKeyBinding() {
        return keyBinding;
    }

    @Override
    public Map<String, ParameterBinding> getParameterBindings() {
        Map<String, ParameterBinding> parameterBindings = new HashMap<>();
        Consumer<ParameterBinding> collector = parameterBinding -> parameterBindings.put(parameterBinding.getFacetName(), parameterBinding);
        keyBinding.getParameterBindings().forEach(collector);
        facetBindings.stream().flatMap(facetBinding -> facetBinding.getParameterBindings().stream()).forEach(collector);
        return parameterBindings;
    }

    @Override
    public Class<P> getPojoType() {
        return pojoType;
    }

    @Override
    public void injectValues(P pojo, Row row, PojoQueryContext context) {
        Iterator<Object> columnValues = CqlUtils.toList(row).iterator();
        keyBinding.injectValues(pojo, columnValues, context);
        facetBindings.forEach(facetBinding -> facetBinding.injectValues(pojo, columnValues, context));
    }

    @Override
    public Insert insertInto(String tableName) {
        Insert insert = QueryBuilder.insertInto(tableName);
        keyBinding.insert(insert);
        facetBindings.forEach(binding -> binding.insert(insert));
        insert.using(QueryBuilder.ttl(bindMarker()));
        return insert;
    }

    @Override
    public Select.Where selectFrom(String tableName) {
        Select.Selection select = QueryBuilder.select();
        keyBinding.select(select);
        facetBindings.forEach(binding -> binding.select(select));
        return select.from(tableName).where();
    }

    @Override
    public Select.Where selectFromByKey(String tableName) {
        Select.Where select = selectFrom(tableName);
        keyBinding.selectWhere(select);
        return select;
    }

    @Override
    public void verifySchema(TableMetadata metadata) {
        keyBinding.verifySchema(metadata);
        facetBindings.forEach(facetBinding -> facetBinding.verifySchema(metadata));
    }

    @Override
    public void visitChildren(P pojo, Predicate<Facet> predicate, PojoVisitor visitor) {
        keyBinding.visitChildren(pojo, predicate, visitor);
        facetBindings.stream().forEach(facetBinding -> facetBinding.visitChildren(pojo, predicate, visitor));
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public void setKeyBinding(KeyBinding keyBinding) {
        this.keyBinding = keyBinding;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultPojoBinding<?> that = (DefaultPojoBinding<?>) o;

        return pojoType.equals(that.pojoType);
    }

    @Override
    public int hashCode() {
        return pojoType.hashCode();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void addFacetBinding(ColumnBinding facetBinding) {
        facetBindings.add(facetBinding);
    }

    protected BoundStatement bind(PreparedStatement statement, List<Object> parameters) {
        return CqlUtils.bind(statement, parameters.toArray(new Object[parameters.size()]));
    }
}
