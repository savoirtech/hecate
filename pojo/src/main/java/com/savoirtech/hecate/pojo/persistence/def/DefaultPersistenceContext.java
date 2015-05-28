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

import com.datastax.driver.core.*;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.statement.StatementOptionsBuilder;
import com.savoirtech.hecate.pojo.cache.def.DefaultPojoCache;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.*;


public class DefaultPersistenceContext implements PersistenceContext {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final StatementOptions defaultOptions;

    private final LoadingCache<PojoMapping<?>, PojoInsert<?>> insertCache = CacheBuilder.newBuilder().build(new InsertCacheLoader());
    private final LoadingCache<PojoMapping<?>, PojoQuery<?>> findByIdCache = CacheBuilder.newBuilder().build(new FindByIdCacheLoader());
    private final LoadingCache<PojoMapping<?>, PojoFindByIds<?>> findByIdsCache = CacheBuilder.newBuilder().build(new FindByIdsCacheLoader());
    private final LoadingCache<PojoMapping<?>, PojoFindForDelete> findForDeleteCache = CacheBuilder.newBuilder().build(new FindForDeleteCacheLoader());
    private final LoadingCache<PojoMapping<?>, PojoDelete<?>> deleteCache = CacheBuilder.newBuilder().build(new DeleteCacheLoader());

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersistenceContext(Session session) {
        this(session, StatementOptionsBuilder.empty());
    }

    public DefaultPersistenceContext(Session session, StatementOptions defaultOptions) {
        this.session = session;
        this.defaultOptions = defaultOptions;
    }

//----------------------------------------------------------------------------------------------------------------------
// PersistenceContext Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Dehydrator createDehydrator(int ttl, StatementOptions options) {
        return new DefaultDehydrator(this, ttl, options);
    }

    @Override
    public Evaporator createEvaporator(StatementOptions options) {
        return new DefaultEvaporator(this, options);
    }

    @Override
    public Hydrator createHydrator(StatementOptions options) {
        // TODO: support pojo cache options!
        return new DefaultHydrator(this, options, new DefaultPojoCache(this));
    }

    @Override
    public <P> PojoDelete delete(PojoMapping<P> mapping) {
        return deleteCache.getUnchecked(mapping);
    }

    @Override
    public ResultSet executeStatement(Statement statement, StatementOptions options) {
        defaultOptions.applyTo(statement);
        options.applyTo(statement);
        return session.execute(statement);
    }

    @Override
    public <P> PojoQueryBuilder<P> find(PojoMapping<P> mapping) {
        return new DefaultPojoQueryBuilder<>(this, mapping);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoQuery<P> findById(PojoMapping<P> mapping) {
        return (PojoQuery<P>) findByIdCache.getUnchecked(mapping);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoFindByIds<P> findByIds(PojoMapping<P> mapping) {
        return (PojoFindByIds<P>) findByIdsCache.getUnchecked(mapping);
    }

    @Override
    public <P> PojoFindForDelete findForDelete(PojoMapping<P> mapping) {
        return findForDeleteCache.getUnchecked(mapping);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoInsert<P> insert(PojoMapping<P> mapping) {
        return (PojoInsert<P>) insertCache.getUnchecked(mapping);
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        return session.prepare(statement);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class DeleteCacheLoader extends CacheLoader<PojoMapping<?>, PojoDelete<?>> {
        @Override
        public PojoDelete<?> load(PojoMapping<?> key) throws Exception {
            return new DefaultPojoDelete<>(DefaultPersistenceContext.this, key);
        }
    }

    private class FindByIdCacheLoader extends CacheLoader<PojoMapping<?>, PojoQuery<?>> {
        @Override
        public PojoQuery<?> load(PojoMapping<?> key) throws Exception {
            return find(key).withName("findById").identifierEquals().build();
        }
    }

    private class FindByIdsCacheLoader extends CacheLoader<PojoMapping<?>, PojoFindByIds<?>> {
        @Override
        public PojoFindByIds<?> load(PojoMapping<?> key) throws Exception {
            return new DefaultPojoFindByIds<>(DefaultPersistenceContext.this, key);
        }
    }

    private class FindForDeleteCacheLoader extends CacheLoader<PojoMapping<?>, PojoFindForDelete> {
        @Override
        public PojoFindForDelete load(PojoMapping<?> key) throws Exception {
            return new DefaultPojoFindForDelete<>(DefaultPersistenceContext.this, key);
        }
    }

    private class InsertCacheLoader extends CacheLoader<PojoMapping<?>, PojoInsert<?>> {
        @Override
        public PojoInsert<?> load(PojoMapping<?> key) throws Exception {
            return new DefaultPojoInsert<>(DefaultPersistenceContext.this, key);
        }
    }
}
