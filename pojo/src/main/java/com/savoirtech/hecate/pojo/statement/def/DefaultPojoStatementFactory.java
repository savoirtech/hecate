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

package com.savoirtech.hecate.pojo.statement.def;

import java.util.function.Function;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;
import com.savoirtech.hecate.pojo.util.CacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultPojoStatementFactory implements PojoStatementFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPojoStatementFactory.class);

    private LoadingCache<CacheKey, PreparedStatement> insertCache = cacheFor(this::loadInsert);
    private LoadingCache<CacheKey, PreparedStatement> deleteCache = cacheFor(this::loadDelete);
    private LoadingCache<CacheKey, PreparedStatement> findByKeyCache = cacheFor(this::loadFindByKey);

    private final Session session;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoStatementFactory(Session session) {
        this.session = session;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoStatementFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PreparedStatement createDelete(PojoBinding<?> binding, String tableName) {
        return deleteCache.getUnchecked(new CacheKey(binding, tableName));
    }

    @Override
    public PreparedStatement createFindByKey(PojoBinding<?> binding, String tableName) {
        return findByKeyCache.getUnchecked(new CacheKey(binding, tableName));
    }

    @Override
    public PreparedStatement createInsert(PojoBinding<?> binding, String tableName) {
        return insertCache.getUnchecked(new CacheKey(binding, tableName));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private <K, V> LoadingCache<K, V> cacheFor(Function<K, V> fn) {
        return CacheBuilder.newBuilder().build(new CacheLoader<K, V>() {
            @Override
            public V load(K key) throws Exception {
                return fn.apply(key);
            }
        });
    }

    private PreparedStatement loadDelete(CacheKey key) {
        Delete.Where delete = key.getBinding().deleteFrom(key.getTableName());
        LOGGER.info("{}.delete(): {}", key.getBinding().getPojoType().getSimpleName(), delete.getQueryString());
        return  session.prepare(delete);
    }

    protected PreparedStatement loadFindByKey(CacheKey key) {
        Select.Where select = key.getBinding().selectFromByKey(key.getTableName());
        LOGGER.info("{}.findByKey(): {}", key.getBinding().getPojoType().getSimpleName(), select.getQueryString());
        return session.prepare(select);
    }

    protected PreparedStatement loadInsert(CacheKey key) {
        Insert insert = key.getBinding().insertInto(key.getTableName());
        LOGGER.info("{}.insert(): {}", key.getBinding().getPojoType().getSimpleName(), insert.getQueryString());
        return session.prepare(insert);
    }
}
