/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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
import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMappingFactory;
import com.savoirtech.hecate.cql3.mapping.def.DefaultPojoMappingFactory;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.Evaporator;
import com.savoirtech.hecate.cql3.persistence.Hydrator;
import com.savoirtech.hecate.cql3.persistence.PersistenceContext;

import java.util.Map;

public class DefaultPersistenceContext implements PersistenceContext {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final PojoMappingFactory pojoMappingFactory;
    private final StatementCache<DefaultPojoQuery<?>> findByKeyCache;
    private final StatementCache<DefaultPojoQuery<?>> findByKeysCache;
    private final StatementCache<DefaultPojoSave> saveCache;
    private final StatementCache<DefaultPojoFindForDelete> findForDeleteCache;
    private final StatementCache<DefaultPojoDelete> deleteCache;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersistenceContext(Session session) {
        this.session = session;
        this.pojoMappingFactory = new DefaultPojoMappingFactory(session);
        this.findByKeysCache = new StatementCache<>(new FindByKeysFactory());
        this.findByKeyCache = new StatementCache<>(new FindByKeyFactory());
        this.saveCache = new StatementCache<>(new SaveFactory());
        this.findForDeleteCache = new StatementCache<>(new FindForDeleteFactory());
        this.deleteCache = new StatementCache<>(new DeleteFactory());
    }

//----------------------------------------------------------------------------------------------------------------------
// PersistenceContext Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DefaultPojoDelete delete(Class<?> pojoType, String tableName) {
        return deleteCache.get(pojoType, tableName);
    }

    @Override
    public <P> DefaultPojoQueryBuilder<P> find(Class<P> pojoType) {
        return find(pojoType, null);
    }

    @Override
    public <P> DefaultPojoQueryBuilder<P> find(Class<P> pojoType, String tableName) {
        return new DefaultPojoQueryBuilder<>(this, pojoMappingFactory.getPojoMapping(pojoType, tableName));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> DefaultPojoQuery<P> findByKey(Class<P> pojoType, String tableName) {
        return (DefaultPojoQuery<P>) findByKeyCache.get(pojoType, tableName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> DefaultPojoQuery<P> findByKeys(Class<P> pojoType, String tableName) {
        return (DefaultPojoQuery<P>) findByKeysCache.get(pojoType, tableName);
    }

    @Override
    public DefaultPojoSave save(Class<?> pojoType, String tableName) {
        return saveCache.get(pojoType, tableName);
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
        return findForDeleteCache.get(pojoType, tableName);
    }

    public Dehydrator newDehydrator() {
        return new DefaultDehydrator(this);
    }

    public Evaporator newEvaporator() {
        return new DefaultEvaporator(this);
    }

    public Hydrator newHydrator() {
        return new DefaultHydrator(this);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private final class DeleteFactory implements StatementFactory<DefaultPojoDelete> {
        @Override
        public DefaultPojoDelete create(PojoMapping pojoMapping) {
            return new DefaultPojoDelete(DefaultPersistenceContext.this, pojoMapping);
        }
    }

    private class FindByKeyFactory implements StatementFactory<DefaultPojoQuery<?>> {
        @Override
        public DefaultPojoQuery<?> create(PojoMapping pojoMapping) {
            return find(pojoMapping).identifierEquals().build();
        }
    }

    private class FindByKeysFactory implements StatementFactory<DefaultPojoQuery<?>> {
        @Override
        public DefaultPojoQuery<?> create(PojoMapping pojoMapping) {
            return find(pojoMapping).identifierIn().build();
        }
    }

    private final class FindForDeleteFactory implements StatementFactory<DefaultPojoFindForDelete> {
        @Override
        public DefaultPojoFindForDelete create(PojoMapping pojoMapping) {
            return new DefaultPojoFindForDelete(DefaultPersistenceContext.this, pojoMapping);
        }
    }

    private final class SaveFactory implements StatementFactory<DefaultPojoSave> {
        @Override
        public DefaultPojoSave create(PojoMapping pojoMapping) {
            return new DefaultPojoSave(DefaultPersistenceContext.this, pojoMapping);
        }
    }

    private final class StatementCache<T> {
        private final Map<String, T> cache;
        private final StatementFactory<T> factory;

        private StatementCache(StatementFactory<T> factory) {
            this.factory = factory;
            this.cache = new MapMaker().makeMap();
        }

        public T get(Class<?> pojoType, String tableName) {
            final String key = keyFor(pojoType, tableName);

            T value = cache.get(key);
            if (value == null) {
                final PojoMapping mapping = pojoMappingFactory.getPojoMapping(pojoType, tableName);
                value = factory.create(mapping);
                cache.put(key, value);
                cache.put(keyFor(pojoType, mapping.getTableName()), value);
            }
            return value;
        }

        private String keyFor(Class<?> pojoType, String tableName) {
            return pojoType.getCanonicalName() + "@" + tableName;
        }
    }

    private interface StatementFactory<T> {
        T create(PojoMapping pojoMapping);
    }
}
