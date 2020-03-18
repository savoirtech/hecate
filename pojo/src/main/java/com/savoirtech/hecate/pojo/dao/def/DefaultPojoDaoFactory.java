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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactory;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryEvent;
import com.savoirtech.hecate.pojo.dao.PojoDaoFactoryListener;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;
import com.savoirtech.hecate.pojo.util.CacheKey;
import com.savoirtech.hecate.pojo.util.FunctionCacheLoader;

public class DefaultPojoDaoFactory implements PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final int DEFAULT_THREAD_POOL_SIZE = 5;

    private final CqlSession session;

    private final PojoBindingFactory bindingFactory;
    private final PojoStatementFactory statementFactory;
    private final PojoQueryContextFactory contextFactory;
    private final NamingStrategy namingStrategy;

    private final List<PojoDaoFactoryListener> listeners = new CopyOnWriteArrayList<>();
    private final LoadingCache<CacheKey, PojoDao<?>> daoCache = CacheBuilder.newBuilder().build(new FunctionCacheLoader<>(this::createPojoDaoInternal));
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory(CqlSession session, PojoBindingFactory bindingFactory, PojoStatementFactory statementFactory, PojoQueryContextFactory contextFactory, NamingStrategy namingStrategy, Executor executor) {
        this.session = session;
        this.bindingFactory = bindingFactory;
        this.statementFactory = statementFactory;
        this.contextFactory = contextFactory;
        this.namingStrategy = namingStrategy;
        this.executor = executor;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DefaultPojoDaoFactory addListener(PojoDaoFactoryListener listener) {
        listeners.add(listener);
        return this;
    }

    @Override
    public <P> PojoDao<P> createPojoDao(Class<P> pojoType) {
        return createPojoDao(pojoType, namingStrategy.getTableName(pojoType));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoDao<P> createPojoDao(Class<P> pojoType, String tableName) {
        PojoBinding<P> pojoBinding = bindingFactory.createPojoBinding(pojoType);
        try {
            return (PojoDao<P>) daoCache.getUnchecked(new CacheKey(pojoBinding, tableName));
        } catch (UncheckedExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), HecateException.class);
            throw e;
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private PojoDao<?> createPojoDaoInternal(CacheKey cacheKey) {
        DefaultPojoDao<?> dao = new DefaultPojoDao<>(session, cacheKey.getBinding(), cacheKey.getTableName(), statementFactory, contextFactory, executor);
        listeners.forEach(listener -> listener.pojoDaoCreated(new PojoDaoFactoryEvent<>((PojoDao<Object>) dao, (PojoBinding<Object>) cacheKey.getBinding(), cacheKey.getTableName())));
        return dao;
    }
}
