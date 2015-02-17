/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.persistence.def;

import com.datastax.driver.core.Session;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMappingFactory;
import com.savoirtech.hecate.cql3.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.Evaporator;
import com.savoirtech.hecate.cql3.persistence.Hydrator;
import com.savoirtech.hecate.cql3.persistence.PersistenceContext;
import com.savoirtech.hecate.cql3.util.PojoCacheKey;

public class DefaultPersistenceContext implements PersistenceContext {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final PojoMappingFactory pojoMappingFactory;
    private final LoadingCache<PojoCacheKey, DefaultPojoQuery<?>> findByKeyCache;
    private final LoadingCache<PojoCacheKey, DefaultPojoQuery<?>> findByKeysCache;
    private final LoadingCache<PojoCacheKey, DefaultPojoSave> saveCache;
    private final LoadingCache<PojoCacheKey, DefaultPojoFindForDelete> findForDeleteCache;
    private final LoadingCache<PojoCacheKey, DefaultPojoDelete> deleteCache;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersistenceContext(Session session) {
        this(session, new DefaultPojoMappingFactory(session));
    }

    public DefaultPersistenceContext(Session session, PojoMappingFactory pojoMappingFactory) {
        this.session = session;
        this.pojoMappingFactory = pojoMappingFactory;
        this.findByKeysCache = CacheBuilder.newBuilder().build(new FindByKeysLoader());
        this.findByKeyCache = CacheBuilder.newBuilder().build(new FindByKeyLoader());
        this.saveCache = CacheBuilder.newBuilder().build(new SaveLoader());
        this.findForDeleteCache = CacheBuilder.newBuilder().build(new FindForDeleteLoader());
        this.deleteCache = CacheBuilder.newBuilder().build(new DeleteLoader());
    }

//----------------------------------------------------------------------------------------------------------------------
// PersistenceContext Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DefaultPojoDelete delete(Class<?> pojoType, String tableName) {
        return deleteCache.getUnchecked(key(pojoType, tableName));
    }

    @Override
    public <P> DefaultPojoQueryBuilder<P> find(Class<P> pojoType) {
        return find(pojoType, null);
    }

    @Override
    public <P> DefaultPojoQueryBuilder<P> find(Class<P> pojoType, String tableName) {
        return new DefaultPojoQueryBuilder<>(this, pojoMappingFactory.getPojoMapping(key(pojoType, tableName)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> DefaultPojoQuery<P> findByKey(Class<P> pojoType, String tableName) {
        return (DefaultPojoQuery<P>) findByKeyCache.getUnchecked(key(pojoType, tableName));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> DefaultPojoQuery<P> findByKeys(Class<P> pojoType, String tableName) {
        return (DefaultPojoQuery<P>) findByKeysCache.getUnchecked(key(pojoType, tableName));
    }

    @Override
    public DefaultPojoSave save(Class<?> pojoType, String tableName) {
        return saveCache.getUnchecked(key(pojoType, tableName));
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Session getSession() {
        return session;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private DefaultPojoQueryBuilder<?> find(PojoMapping mapping) {
        return new DefaultPojoQueryBuilder<>(this, mapping);
    }

    public DefaultPojoFindForDelete findForDelete(Class<?> pojoType, String tableName) {
        return findForDeleteCache.getUnchecked(key(pojoType, tableName));
    }

    private PojoCacheKey key(Class<?> pojoType, String tableName) {
        return new PojoCacheKey(pojoType, tableName);
    }

    public Dehydrator newDehydrator(Integer ttl) {
        return new DefaultDehydrator(this, ttl);
    }

    public Evaporator newEvaporator() {
        return new DefaultEvaporator(this);
    }

    public Hydrator newHydrator() {
        return new DefaultHydrator(this);
    }

    private PojoMapping pojoMapping(PojoCacheKey key) {
        return pojoMappingFactory.getPojoMapping(key);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private final class DeleteLoader extends PojoMappingCacheLoader<DefaultPojoDelete> {
        @Override
        protected DefaultPojoDelete load(PojoMapping pojoMapping) {
            return new DefaultPojoDelete(DefaultPersistenceContext.this, pojoMapping);
        }
    }

    private class FindByKeyLoader extends PojoMappingCacheLoader<DefaultPojoQuery<?>> {
        @Override
        protected DefaultPojoQuery<?> load(PojoMapping pojoMapping) {
            return find(pojoMapping).identifierEquals().build();
        }
    }

    private class FindByKeysLoader extends PojoMappingCacheLoader<DefaultPojoQuery<?>> {
        @Override
        protected DefaultPojoQuery<?> load(PojoMapping pojoMapping) {
            return find(pojoMapping).identifierIn().build();
        }
    }

    private final class FindForDeleteLoader extends PojoMappingCacheLoader<DefaultPojoFindForDelete> {
        @Override
        protected DefaultPojoFindForDelete load(PojoMapping pojoMapping) {
            return new DefaultPojoFindForDelete(DefaultPersistenceContext.this, pojoMapping);
        }
    }

    private abstract class PojoMappingCacheLoader<T> extends CacheLoader<PojoCacheKey, T> {
        @Override
        public final T load(PojoCacheKey key) throws Exception {
            return load(pojoMapping(key));
        }

        protected abstract T load(PojoMapping pojoMapping);
    }

    private final class SaveLoader extends PojoMappingCacheLoader<DefaultPojoSave> {
        @Override
        protected DefaultPojoSave load(PojoMapping pojoMapping) {
            return new DefaultPojoSave(DefaultPersistenceContext.this, pojoMapping);
        }
    }
}
