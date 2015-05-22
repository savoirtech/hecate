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
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMappingFactory;
import com.savoirtech.hecate.pojo.persistence.*;

import java.util.List;
import java.util.function.Consumer;


public class DefaultPersistenceContext implements PersistenceContext {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final PojoMappingFactory pojoMappingFactory;
    private final List<Consumer<Statement>> defaultStatementModifiers;

    private final LoadingCache<PojoMapping<?>, PojoInsert<?>> insertCache = CacheBuilder.newBuilder().build(new InsertCacheLoader());
    private final LoadingCache<PojoMapping<?>,PojoQuery<?>> findByIdCache = CacheBuilder.newBuilder().build(new FindByIdCacheLoader());
    private final LoadingCache<PojoMapping<?>,PojoQuery<?>> findByIdsCache = CacheBuilder.newBuilder().build(new FindByIdsCacheLoader());

    private final LoadingCache<PojoMapping<?>,PojoDelete<?>> deleteCache = CacheBuilder.newBuilder().build(new DeleteCacheLoader());
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------


    public DefaultPersistenceContext(Session session, PojoMappingFactory pojoMappingFactory, List<Consumer<Statement>> defaultStatementModifiers) {
        this.session = session;
        this.pojoMappingFactory = pojoMappingFactory;
        this.defaultStatementModifiers = defaultStatementModifiers;
    }

//----------------------------------------------------------------------------------------------------------------------
// PersistenceContext Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Dehydrator createDehydrator() {
        return new DefaultDehydrator(this);
    }

    @Override
    public Hydrator createHydrator() {
        return new DefaultHydrator(this);
    }

    @Override
    public ResultSet executeStatement(Statement statement, List<Consumer<Statement>> statementModifiers) {
        defaultStatementModifiers.stream().forEach(mod -> mod.accept(statement));
        statementModifiers.stream().forEach(mod -> mod.accept(statement));
        return session.execute(statement);
    }

    @Override
    public Evaporator createEvaporator() {
        return new DefaultEvaporator(this);
    }

    @Override
    public <P> PojoDelete delete(PojoMapping<P> mapping) {
        return deleteCache.getUnchecked(mapping);
    }

    @Override
    public <P> PojoQueryBuilder<P> find(PojoMapping<P> mapping) {
        return new DefaultPojoQueryBuilder<>(this, mapping);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoQuery<P> findById(PojoMapping<P> mapping) {
        return (PojoQuery<P>)findByIdCache.getUnchecked(mapping);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoQuery<P> findByIds(PojoMapping<P> mapping) {
        return (PojoQuery<P>)findByIdsCache.getUnchecked(mapping);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoInsert<P> insert(PojoMapping<P> mapping) {
        return (PojoInsert<P>)insertCache.getUnchecked(mapping);
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        return session.prepare(statement);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class FindByIdCacheLoader extends CacheLoader<PojoMapping<?>, PojoQuery<?>> {
        @Override
        public PojoQuery<?> load(PojoMapping<?> key) throws Exception {
            return find(key).identifierEquals().build();
        }
    }

    private class FindByIdsCacheLoader extends CacheLoader<PojoMapping<?>, PojoQuery<?>> {
        @Override
        public PojoQuery<?> load(PojoMapping<?> key) throws Exception {
            return find(key).identifierIn().build();
        }
    }

    private class InsertCacheLoader extends CacheLoader<PojoMapping<?>, PojoInsert<?>> {
        @Override
        public PojoInsert<?> load(PojoMapping<?> key) throws Exception {
            return new DefaultPojoInsert<>(DefaultPersistenceContext.this, key);
        }
    }

    private class DeleteCacheLoader extends CacheLoader<PojoMapping<?>, PojoDelete<?>> {
        @Override
        public PojoDelete<?> load(PojoMapping<?> key) throws Exception {
            return new DefaultPojoDelete<>(DefaultPersistenceContext.this, key);
        }
    }
}
