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

package com.savoirtech.hecate.pojo.cache.def;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.pojo.cache.PojoCache;
import com.savoirtech.hecate.pojo.exception.PojoNotFoundException;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.metrics.PojoMetricsUtils;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultPojoCache implements PojoCache {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final int DEFAULT_MAX_SIZE = 5000;

    private final PersistenceContext persistenceContext;

    private final LoadingCache<PojoMapping<?>, LoadingCache<Object, Optional<Object>>> caches = CacheBuilder.newBuilder().build(new OuterCacheLoader());

    private final int defaultMaxCacheSize;
    private final Map<PojoMapping<?>, Integer> maxCacheSizes;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoCache(PersistenceContext persistenceContext) {
        this(persistenceContext, Collections.emptyMap(), DEFAULT_MAX_SIZE);
    }

    public DefaultPojoCache(PersistenceContext persistenceContext, Map<PojoMapping<?>, Integer> maxCacheSizes) {
        this(persistenceContext, maxCacheSizes, DEFAULT_MAX_SIZE);
    }

    public DefaultPojoCache(PersistenceContext persistenceContext, int defaultMaxCacheSize) {
        this(persistenceContext, new HashMap<>(), defaultMaxCacheSize);
    }

    public DefaultPojoCache(PersistenceContext persistenceContext, Map<PojoMapping<?>, Integer> maxCacheSizes, int defaultMaxCacheSize) {
        this.persistenceContext = persistenceContext;
        this.maxCacheSizes = maxCacheSizes;
        this.defaultMaxCacheSize = defaultMaxCacheSize;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoCache Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean contains(PojoMapping<?> mapping) {
        return size(mapping) > 0;
    }

    @Override
    public Set<Object> idSet(PojoMapping<?> mapping) {
        LoadingCache<Object, Optional<Object>> cache = cacheForMapping(mapping);
        return cache == null ? Collections.emptySet() : cache.asMap().keySet();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> P lookup(PojoMapping<P> mapping, Object id) {
        Optional<Object> optional = caches.getUnchecked(mapping).getUnchecked(id);
        if(optional.isPresent())
        {
            return (P)optional.get();
        }
        throw new PojoNotFoundException(mapping, id);
    }

    @Override
    public <P> void put(PojoMapping<P> mapping, Object id, P pojo) {
        if (pojo != null) {
            caches.getUnchecked(mapping).put(id, Optional.of(pojo));
        }
    }

    @Override
    public <P> void putAll(PojoMapping<P> mapping, Iterable<P> pojos) {
        LoadingCache<Object, Optional<Object>> cache = caches.getUnchecked(mapping);
        for (P pojo : pojos) {
            if (pojo != null) {
                cache.put(mapping.getForeignKeyMapping().getColumnValue(pojo), Optional.of(pojo));
            }
        }
    }

    @Override
    public long size(PojoMapping<?> mapping) {
        LoadingCache<Object, Optional<Object>> cache = cacheForMapping(mapping);
        return cache == null ? 0 : cache.size();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected <P> LoadingCache<Object, Optional<Object>> cacheForMapping(PojoMapping<P> mapping) {
        return caches.getIfPresent(mapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class InnerCacheLoader extends CacheLoader<Object, Optional<Object>> {
        private final PojoMapping<?> mapping;

        public InnerCacheLoader(PojoMapping<?> mapping) {
            this.mapping = mapping;
        }

        @Override
        public Optional<Object> load(Object id) throws Exception {
            PojoMetricsUtils.createCounter(mapping, "cacheMiss").inc();
            Object pojo = persistenceContext.findById(mapping).execute(id).one();
            return Optional.fromNullable(pojo);
        }
    }

    private class OuterCacheLoader extends CacheLoader<PojoMapping<?>, LoadingCache<Object, Optional<Object>>> {
        @Override
        public LoadingCache<Object, Optional<Object>> load(final PojoMapping<?> mapping) throws Exception {
            return CacheBuilder.newBuilder().maximumSize(maxCacheSizes.getOrDefault(mapping, defaultMaxCacheSize)).build(new InnerCacheLoader(mapping));
        }
    }
}
