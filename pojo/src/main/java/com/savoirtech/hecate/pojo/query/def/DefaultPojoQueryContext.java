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

package com.savoirtech.hecate.pojo.query.def;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import com.datastax.driver.core.*;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.exception.PojoNotFoundException;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;
import com.savoirtech.hecate.pojo.util.CacheKey;
import com.savoirtech.hecate.pojo.util.FunctionCacheLoader;

public class DefaultPojoQueryContext implements PojoQueryContext {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final LoadingCache<CacheKey, LoadingCache<List<Object>, Object>> pojoCache;
    private final Session session;
    private final PojoStatementFactory statementFactory;
    private final Queue<HydratedPojo<?>> hydratedQueue = new ConcurrentLinkedQueue<>();
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQueryContext(Session session, PojoStatementFactory statementFactory, int maximumCacheSize, Executor executor) {
        this.session = session;
        this.statementFactory = statementFactory;
        this.pojoCache = CacheBuilder.newBuilder().build(FunctionCacheLoader.loader(key -> CacheBuilder.newBuilder().maximumSize(maximumCacheSize).build(new PojoCacheLoader<>(key.getBinding(), key.getTableName()))));
        this.executor = executor;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoQueryContext Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void awaitCompletion() {
        while (!hydratedQueue.isEmpty()) {
            HydratedPojo<?> hydratedPojo = hydratedQueue.remove();
            if(!Futures.getUnchecked(hydratedPojo.getFuture())) {
                throw new PojoNotFoundException(hydratedPojo.getBinding(), hydratedPojo.getTableName(), hydratedPojo.getKeys());
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> P createPojo(PojoBinding<P> binding, String tableName, List<Object> keys) {
        return (P) pojoCache.getUnchecked(new CacheKey(binding, tableName)).getUnchecked(keys);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class HydratedPojo<P> {
        private final PojoBinding<P> binding;
        private final String tableName;
        private final List<Object> keys;
        private final ListenableFuture<Boolean> future;

        public HydratedPojo(PojoBinding<P> binding, String tableName, List<Object> keys, ListenableFuture<Boolean> future) {
            this.binding = binding;
            this.tableName = tableName;
            this.keys = keys;
            this.future = future;
        }

        public PojoBinding<P> getBinding() {
            return binding;
        }

        public String getTableName() {
            return tableName;
        }

        public List<Object> getKeys() {
            return keys;
        }

        public ListenableFuture<Boolean> getFuture() {
            return future;
        }
    }

    private class PojoCacheLoader<P> extends CacheLoader<List<Object>, Object> {
        private final PojoBinding<P> binding;
        private final String tableName;

        public PojoCacheLoader(PojoBinding<P> binding, String tableName) {
            this.binding = binding;
            this.tableName = tableName;
        }

        @Override
        public Object load(List<Object> keys) throws Exception {
            P pojo = binding.createPojo();
            binding.getKeyBinding().injectValues(pojo, keys.iterator(), DefaultPojoQueryContext.this);
            PreparedStatement statement = statementFactory.createFindByKey(binding, tableName);
            ResultSetFuture rsFuture = session.executeAsync(binding.bindWhereIdEquals(statement, keys));
            ListenableFuture<Boolean> future = Futures.transform(rsFuture, (Function<ResultSet, Boolean>) resultSet -> {
                Row row = resultSet.one();
                if (row == null) {
                    return Boolean.FALSE;
                }
                binding.injectValues(pojo, row, DefaultPojoQueryContext.this);
                return Boolean.TRUE;
            }, executor);
            hydratedQueue.add(new HydratedPojo<>(binding, tableName, keys, future));
            return pojo;
        }
    }
}
