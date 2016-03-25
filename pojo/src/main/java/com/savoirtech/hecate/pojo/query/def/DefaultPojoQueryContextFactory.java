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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import com.savoirtech.hecate.pojo.query.PojoQueryContextFactory;
import com.savoirtech.hecate.pojo.statement.PojoStatementFactory;

public class DefaultPojoQueryContextFactory implements PojoQueryContextFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final int DEFAULT_MAX_CACHE_SIZE = 5000;
    public static final int DEFAULT_THREAD_POOL_SIZE = 5;

    private final Session session;
    private final PojoStatementFactory statementFactory;
    private final int maximumCacheSize;
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQueryContextFactory(Session session, PojoStatementFactory statementFactory) {
        this(session, statementFactory, DEFAULT_MAX_CACHE_SIZE, DEFAULT_THREAD_POOL_SIZE);
    }

    public DefaultPojoQueryContextFactory(Session session, PojoStatementFactory statementFactory, int maximumCacheSize, int threadPoolSize) {
        this.session = session;
        this.statementFactory = statementFactory;
        this.maximumCacheSize = maximumCacheSize;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoQueryContextFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoQueryContext createPojoQueryContext() {
        return new DefaultPojoQueryContext(session, statementFactory, maximumCacheSize, executor);
    }
}
