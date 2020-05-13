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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.Table;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.ParameterBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.exception.SchemaVerificationException;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import com.savoirtech.hecate.pojo.reflect.ReflectionUtils;

public class DefaultPojoBinding<P> implements PojoBinding<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

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
    public Delete deleteFrom(String tableName) {
        DeleteSelection deleteSelection = QueryBuilder.deleteFrom(tableName);
        return keyBinding.delete(deleteSelection);
    }

    @Override
    public void describe(Table table, Schema schema) {
        keyBinding.describe(table, schema);
        facetBindings.forEach(binding -> binding.describe(table, schema));
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
        InsertInto insertInto = QueryBuilder.insertInto(tableName);
        RegularInsert insert = keyBinding.insert(insertInto);
        for (ColumnBinding columnBinding : facetBindings) {
            insert = columnBinding.insert(insert);
        }
        return insert.usingTtl(bindMarker());
    }

    @Override
    public Select selectFrom(String tableName) {
        SelectFrom selectFrom = QueryBuilder.selectFrom(tableName);
        Select select = keyBinding.select(selectFrom);
        for (ColumnBinding columnBinding : facetBindings) {
            select = columnBinding.select(select);
        }
        return select;
    }

    @Override
    public Select selectFromByKey(String tableName) {
        Select select = selectFrom(tableName);
        return keyBinding.selectWhere(select);
    }

    @Override
    public void verifySchema(KeyspaceMetadata keyspaceMetadata, String tableName) {
        Optional<TableMetadata> tableMetadata = keyspaceMetadata.getTable(tableName);
        if(!tableMetadata.isPresent()) {
            throw new SchemaVerificationException("Table \"%s\" not found in keyspace \"%s\".", tableName, keyspaceMetadata.getName());
        }
        keyBinding.verifySchema(keyspaceMetadata, tableMetadata.get());
        facetBindings.forEach(facetBinding -> facetBinding.verifySchema(keyspaceMetadata, tableMetadata.get()));
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
