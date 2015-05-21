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
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoInsert;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;


public class DefaultPersistenceContext implements PersistenceContext {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;

    private final LoadingCache<PojoMapping<?>, PojoInsert<?>> insertCache = CacheBuilder.newBuilder().build(new InsertCacheLoader());

    private final List<Consumer<Statement>> defaultStatementModifiers;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    @SafeVarargs
    public DefaultPersistenceContext(Session session, Consumer<Statement>... statementModifiers) {
        this.session = session;
        this.defaultStatementModifiers = Arrays.asList(statementModifiers);
    }

//----------------------------------------------------------------------------------------------------------------------
// PersistenceContext Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Dehydrator createDehydrator() {
        return new DefaultDehydrator(this);
    }

    @Override
    public ResultSet executeStatement(Statement statement, Consumer<Statement>... statementModifiers) {
        defaultStatementModifiers.stream().forEach(mod -> mod.accept(statement));
        Arrays.stream(statementModifiers).forEach(mod -> mod.accept(statement));
        return session.execute(statement);
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

    private class InsertCacheLoader extends CacheLoader<PojoMapping<?>, PojoInsert<?>> {
        @Override
        public PojoInsert<?> load(PojoMapping<?> key) throws Exception {
            return new DefaultPojoInsert<>(DefaultPersistenceContext.this, key);
        }
    }
}
