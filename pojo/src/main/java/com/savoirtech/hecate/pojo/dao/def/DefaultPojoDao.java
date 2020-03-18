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

package com.savoirtech.hecate.pojo.dao.def;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.savoirtech.hecate.annotation.Ttl;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.statement.StatementOptionsBuilder;
import com.savoirtech.hecate.core.update.AsyncUpdateGroup;
import com.savoirtech.hecate.core.update.UpdateGroup;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoMultiQuery;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.query.PojoQueryBuilder;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.query.custom.CustomPojoQuery;
import com.savoirtech.hecate.pojo.query.def.DefaultPojoQueryBuilder;
import com.savoirtech.hecate.pojo.query.finder.FindByKeyQuery;
import com.savoirtech.hecate.pojo.query.mapper.PojoQueryRowMapper;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;

public class DefaultPojoDao<P> implements PojoDao<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final int NO_TTL = 0;

    private final CqlSession session;
    private final PojoBinding<P> binding;
    private final String tableName;
    private final PojoStatementFactory statementFactory;
    private final PojoQueryContextFactory contextFactory;
    private final int defaultTtl;
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(CqlSession session, PojoBinding<P> binding, String tableName, PojoStatementFactory statementFactory, PojoQueryContextFactory contextFactory, Executor executor) {
        this.session = session;
        this.binding = binding;
        this.tableName = tableName;
        this.statementFactory = statementFactory;
        this.contextFactory = contextFactory;
        this.defaultTtl = ttl(binding.getPojoType());
        this.executor = executor;
    }

    private static int ttl(Class<?> pojoType) {
        Ttl annotation = pojoType.getAnnotation(Ttl.class);
        return annotation != null ? annotation.value() : NO_TTL;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDao Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void delete(P pojo) {
        completeSync(group -> delete(group, StatementOptionsBuilder.empty(), pojo));
    }

    @Override
    public void delete(StatementOptions options, P pojo) {
        completeSync(group -> delete(group, options, pojo));
    }

    @Override
    public void delete(UpdateGroup group, P pojo) {
        delete(group, StatementOptionsBuilder.empty(), pojo);
    }

    @Override
    public void delete(UpdateGroup group, StatementOptions options, P pojo) {
        if (pojo != null) {
            deletePojo(pojo, binding, tableName, group, options);
            binding.visitChildren(pojo, Facet::isCascadeDelete, new DeleteVisitor(group, options));
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(P pojo) {
        return completeAsync(group -> delete(group, StatementOptionsBuilder.empty(), pojo));
    }

    @Override
    public CompletableFuture<Void> deleteAsync(StatementOptions options, P pojo) {
        return completeAsync(group -> delete(group, options, pojo));
    }

    @Override
    public void deleteByKey(Object... values) {
        delete(findByKey(values));
    }

    @Override
    public void deleteByKey(StatementOptions options, Object... values) {
        delete(options, findByKey(values));
    }

    @Override
    public void deleteByKey(UpdateGroup group, StatementOptions options, Object... values) {
        delete(group, options, findByKey(values));
    }

    @Override
    public CompletableFuture<Void> deleteByKeyAsync(Object... values) {
        return completeAsync(group -> deleteByKey(group, StatementOptionsBuilder.empty(), values));
    }

    @Override
    public CompletableFuture<Void> deleteByKeyAsync(StatementOptions options, Object... values) {
        return completeAsync(group -> deleteByKey(group, options, values));
    }

    @Override
    public PojoQueryBuilder<P> find() {
        return new DefaultPojoQueryBuilder<>(session, binding, tableName, contextFactory, executor);
    }

    @Override
    public PojoQuery<P> find(Function<Select, Select> builder) {
        Select where = binding.selectFrom(tableName);
        Select select = builder.apply(where);
        return new CustomPojoQuery<>(session, binding, contextFactory, select, executor);
    }

    @Override
    public P findByKey(Object... values) {
        return findByKey(StatementOptionsBuilder.empty(), values);
    }

    @Override
    public P findByKey(StatementOptions options, Object... values) {
        PreparedStatement statement = statementFactory.createFindByKey(binding, tableName);
        BoundStatement boundStatement = binding.bindWhereIdEquals(statement, Arrays.asList(values));
        return new MappedQueryResult<>(session.execute(boundStatement), new PojoQueryRowMapper<>(binding, contextFactory.createPojoQueryContext(executor))).one();
    }

    @Override
    public PojoMultiQuery<P> findByKeys() {
        return findByKeys(StatementOptionsBuilder.empty());
    }

    @Override
    public PojoMultiQuery<P> findByKeys(StatementOptions options) {
        return new FindByKeyQuery<>(session, binding, contextFactory, statementFactory.createFindByKey(binding, tableName), executor).multi(options);
    }

    @Override
    public void save(P pojo) {
        completeSync(group -> save(group, StatementOptionsBuilder.empty(), pojo, defaultTtl));
    }

    @Override
    public void save(P pojo, int ttl) {
        completeSync(group -> save(group, StatementOptionsBuilder.empty(), pojo, ttl));
    }

    @Override
    public void save(StatementOptions options, P pojo) {
        completeSync(group -> save(group, options, pojo, defaultTtl));
    }

    @Override
    public void save(UpdateGroup group, P pojo) {
        save(group, StatementOptionsBuilder.empty(), pojo, defaultTtl);
    }

    @Override
    public void save(StatementOptions options, P pojo, int ttl) {
        completeSync(group -> save(group, options, pojo, ttl));
    }

    @Override
    public void save(UpdateGroup group, P pojo, int ttl) {
        save(group, StatementOptionsBuilder.empty(), pojo, ttl);
    }

    @Override
    public void save(UpdateGroup group, StatementOptions options, P pojo) {
        save(group, options, pojo, defaultTtl);
    }

    @Override
    public void save(UpdateGroup group, StatementOptions options, P pojo, int ttl) {
        insertPojo(pojo, binding, tableName, group, options, ttl);
        binding.visitChildren(pojo, Facet::isCascadeSave, new SaveVisitor(group, options, ttl));
    }

    @Override
    public CompletableFuture<Void> saveAsync(P pojo) {
        return completeAsync(group -> save(group, StatementOptionsBuilder.empty(), pojo, defaultTtl));
    }

    @Override
    public CompletableFuture<Void> saveAsync(P pojo, int ttl) {
        return completeAsync(group -> save(group, StatementOptionsBuilder.empty(), pojo, ttl));
    }

    @Override
    public CompletableFuture<Void> saveAsync(StatementOptions options, P pojo) {
        return completeAsync(group -> save(group, options, pojo, defaultTtl));
    }

    @Override
    public CompletableFuture<Void> saveAsync(StatementOptions options, P pojo, int ttl) {
        return completeAsync(group -> save(group, options, pojo, ttl));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private CompletableFuture<Void> completeAsync(Consumer<UpdateGroup> consumer) {
        UpdateGroup group = new AsyncUpdateGroup(session);
        consumer.accept(group);
        return group.completeAsync();
    }

    private void completeSync(Consumer<UpdateGroup> consumer) {
        UpdateGroup group = new AsyncUpdateGroup(session);
        consumer.accept(group);
        group.complete();
    }

    private <T> void deletePojo(T pojo, PojoBinding<T> pojoBinding, String tableName, UpdateGroup group, StatementOptions options) {
        PreparedStatement delete = statementFactory.createDelete(pojoBinding, tableName);
        List<Object> keys = new LinkedList<>();
        pojoBinding.getKeyBinding().collectParameters(pojo, keys);
        BoundStatement statement = pojoBinding.bindWhereIdEquals(delete, keys);
        Statement optionsApplied = options.applyTo(statement);
        group.addUpdate(optionsApplied);
    }

    private <T> void insertPojo(T pojo, PojoBinding<T> pojoBinding, String tableName, UpdateGroup group, StatementOptions options, int ttl) {
        PreparedStatement insert = statementFactory.createInsert(pojoBinding, tableName);
        BoundStatement statement = pojoBinding.bindInsert(insert, pojo, ttl);
        Statement optionsApplied = options.applyTo(statement);
        group.addUpdate(optionsApplied);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class DeleteVisitor implements PojoVisitor {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final UpdateGroup group;
        private final StatementOptions options;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public DeleteVisitor(UpdateGroup group, StatementOptions options) {
            this.group = group;
            this.options = options;
        }

//----------------------------------------------------------------------------------------------------------------------
// PojoVisitor Implementation
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public <T> void visit(T pojo, PojoBinding<T> pojoBinding, String tableName, Predicate<Facet> predicate) {
            deletePojo(pojo, pojoBinding, tableName, group, options);
            pojoBinding.visitChildren(pojo, predicate, this);
        }
    }

    private class SaveVisitor implements PojoVisitor {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final UpdateGroup group;
        private final StatementOptions options;
        private final int ttl;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public SaveVisitor(UpdateGroup group, StatementOptions options, int ttl) {
            this.group = group;
            this.options = options;
            this.ttl = ttl;
        }

//----------------------------------------------------------------------------------------------------------------------
// PojoVisitor Implementation
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public <T> void visit(T pojo, PojoBinding<T> pojoBinding, String tableName, Predicate<Facet> predicate) {
            insertPojo(pojo, pojoBinding, tableName, group, options, ttl);
            pojoBinding.visitChildren(pojo, predicate, this);
        }
    }
}
