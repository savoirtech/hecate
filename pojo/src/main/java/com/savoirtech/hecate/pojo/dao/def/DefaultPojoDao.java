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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
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
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.query.PojoQueryBuilder;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.query.custom.CustomPojoQuery;
import com.savoirtech.hecate.pojo.query.def.DefaultPojoQueryBuilder;
import com.savoirtech.hecate.pojo.query.mapper.PojoQueryRowMapper;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;

public class DefaultPojoDao<P> implements PojoDao<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final int NO_TTL = 0;

    private final Session session;
    private final PojoBinding<P> binding;
    private final String tableName;
    private final PojoStatementFactory statementFactory;
    private final PojoQueryContextFactory contextFactory;
    private final int defaultTtl;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(Session session, PojoBinding<P> binding, String tableName, PojoStatementFactory statementFactory, PojoQueryContextFactory contextFactory) {
        this.session = session;
        this.binding = binding;
        this.tableName = tableName;
        this.statementFactory = statementFactory;
        this.contextFactory = contextFactory;
        this.defaultTtl = ttl(binding.getPojoType());
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
        delete(StatementOptionsBuilder.empty(), pojo);
    }

    @Override
    public void delete(StatementOptions options, P pojo) {
        async(group -> delete(group, options, pojo));
    }

    @Override
    public void delete(UpdateGroup group, StatementOptions options, P pojo) {
        deletePojo(pojo, binding, tableName, group, options);
        binding.visitChildren(pojo, Facet::isCascadeDelete, new DeleteVisitor(group, options));
    }

    @Override
    public void deleteByKeys(Object... keys) {
        delete(findByKeys(keys));
    }

    @Override
    public void deleteByKeys(StatementOptions options, Object... keys) {
        delete(options, findByKeys(keys));
    }

    @Override
    public void deleteByKeys(UpdateGroup group, StatementOptions options, Object... keys) {
        delete(group, options, findByKeys(keys));
    }

    @Override
    public PojoQueryBuilder<P> find() {
        return new DefaultPojoQueryBuilder<>(session, binding, tableName, contextFactory);
    }

    @Override
    public PojoQuery<P> find(Consumer<Select.Where> builder) {
        Select.Where where = binding.selectFrom(tableName);
        builder.accept(where);
        return new CustomPojoQuery<>(session, binding, contextFactory, where);
    }

    @Override
    public P findByKeys(Object... keys) {
        return findByKeys(StatementOptionsBuilder.empty(), keys);
    }

    @Override
    public P findByKeys(StatementOptions options, Object... keys) {
        PreparedStatement statement = statementFactory.createFindByKey(binding, tableName);
        BoundStatement boundStatement = binding.bindWhereIdEquals(statement, Arrays.asList(keys));
        return new MappedQueryResult<>(session.execute(boundStatement), new PojoQueryRowMapper<>(binding, contextFactory.createPojoQueryContext())).one();
    }

    @Override
    public void save(P pojo) {
        async(group -> save(group, StatementOptionsBuilder.empty(), pojo, defaultTtl));
    }

    @Override
    public void save(P pojo, int ttl) {
        async(group -> save(group, StatementOptionsBuilder.empty(), pojo, ttl));
    }

    @Override
    public void save(StatementOptions options, P pojo) {
        async(group -> save(group, options, pojo, defaultTtl));
    }

    @Override
    public void save(UpdateGroup group, P pojo) {
        save(group, StatementOptionsBuilder.empty(), pojo, defaultTtl);
    }

    @Override
    public void save(StatementOptions options, P pojo, int ttl) {
        async(group -> save(group, options, pojo, ttl));
    }

    @Override
    public void save(UpdateGroup group, P pojo, int ttl) {
        save(group, StatementOptionsBuilder.empty(), pojo, ttl);
    }

    @Override
    public void save(UpdateGroup group, StatementOptions options, P pojo) {
        save(group, StatementOptionsBuilder.empty(), pojo, defaultTtl);
    }

    @Override
    public void save(UpdateGroup group, StatementOptions options, P pojo, int ttl) {
        insertPojo(pojo, binding, tableName, group, options, ttl);
        binding.visitChildren(pojo, Facet::isCascadeSave, new SaveVisitor(group, options, ttl));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private void async(Consumer<UpdateGroup> consumer) {
        UpdateGroup group = new AsyncUpdateGroup(session);
        consumer.accept(group);
        group.complete();
    }

    private <T> void deletePojo(T pojo, PojoBinding<T> pojoBinding, String tableName, UpdateGroup group, StatementOptions options) {
        PreparedStatement delete = statementFactory.createDelete(pojoBinding, tableName);
        List<Object> keys = new LinkedList<>();
        pojoBinding.getKeyBinding().collectParameters(pojo, keys);
        BoundStatement statement = pojoBinding.bindWhereIdEquals(delete, keys);
        options.applyTo(statement);
        group.addUpdate(statement);
    }

    private <T> void insertPojo(T pojo, PojoBinding<T> pojoBinding, String tableName, UpdateGroup group, StatementOptions options, int ttl) {
        PreparedStatement insert = statementFactory.createInsert(pojoBinding, tableName);
        BoundStatement statement = pojoBinding.bindInsert(insert, pojo, ttl);
        options.applyTo(statement);
        group.addUpdate(statement);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class DeleteVisitor implements PojoVisitor {
        private final UpdateGroup group;
        private final StatementOptions options;

        public DeleteVisitor(UpdateGroup group, StatementOptions options) {
            this.group = group;
            this.options = options;
        }

        @Override
        public <T> void visit(T pojo, PojoBinding<T> pojoBinding, String tableName, Predicate<Facet> predicate) {
            deletePojo(pojo, pojoBinding, tableName, group, options);
            pojoBinding.visitChildren(pojo, predicate, this);
        }
    }

    private class SaveVisitor implements PojoVisitor {
        private final UpdateGroup group;
        private final StatementOptions options;
        private final int ttl;

        public SaveVisitor(UpdateGroup group, StatementOptions options, int ttl) {
            this.group = group;
            this.options = options;
            this.ttl = ttl;
        }

        @Override
        public <T> void visit(T pojo, PojoBinding<T> pojoBinding, String tableName, Predicate<Facet> predicate) {
            insertPojo(pojo, pojoBinding, tableName, group, options, ttl);
            pojoBinding.visitChildren(pojo, predicate, this);

        }
    }
}
