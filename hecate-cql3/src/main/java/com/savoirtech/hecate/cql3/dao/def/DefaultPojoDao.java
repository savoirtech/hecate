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

package com.savoirtech.hecate.cql3.dao.def;

import java.util.Arrays;
import java.util.List;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.persistence.def.DefaultPersistenceContext;

public class DefaultPojoDao<K, P> implements PojoDao<K, P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final DefaultPersistenceContext persistenceContext;

    private final Class<P> rootPojoType;
    private final String rootTableName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(DefaultPersistenceContext persistenceContext, Class<P> rootPojoType, String rootTableName) {
        this.persistenceContext = persistenceContext;
        this.rootPojoType = rootPojoType;
        this.rootTableName = rootTableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDao Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void delete(K key) {
        persistenceContext.delete(rootPojoType, rootTableName).execute(Arrays.<Object>asList(key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public P findByKey(K key) {
        return persistenceContext.findByKey(rootPojoType, rootTableName).execute(persistenceContext.newHydrator(), key).one();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<P> findByKeys(Iterable<K> keys) {
        return persistenceContext.findByKeys(rootPojoType, rootTableName).execute(persistenceContext.newHydrator(), keys).list();
    }

    @Override
    public void save(P pojo) {
        persistenceContext.save(rootPojoType, rootTableName).execute(pojo);
    }

    @Override
    public void save(P pojo, int ttl) { persistenceContext.save(rootPojoType, rootTableName,ttl).execute(pojo); }
}
