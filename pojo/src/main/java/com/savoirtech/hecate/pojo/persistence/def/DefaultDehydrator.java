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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoInsert;

public class DefaultDehydrator implements Dehydrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final Multimap<PojoMapping<? extends Object>, Object> agenda = MultimapBuilder.hashKeys().linkedListValues().build();
    private final int ttl;
    private final StatementOptions options;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultDehydrator(PersistenceContext persistenceContext, int ttl, StatementOptions options) {
        this.persistenceContext = persistenceContext;
        this.ttl = ttl;
        this.options = options;
    }

//----------------------------------------------------------------------------------------------------------------------
// Dehydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public void dehydrate(PojoMapping<?> pojoMapping, Iterable<?> pojos) {
        agenda.putAll(pojoMapping, pojos);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute() {
        List<ResultSetFuture> futures = new LinkedList<>();
        while (!agenda.isEmpty()) {
            PojoMapping<? extends Object> mapping = agenda.keySet().iterator().next();
            Collection<Object> pojos = agenda.removeAll(mapping);
            PojoInsert<Object> insert = persistenceContext.insert((PojoMapping<Object>) mapping);
            pojos.forEach(pojo -> futures.add(insert.insert(pojo, this, ttl, options)));
        }
        try {
            Uninterruptibles.getUninterruptibly(Futures.allAsList(futures));
        } catch (ExecutionException e) {
            throw new HecateException(e, "An error occurred while waiting for insert statements to finish.");
        }
    }
}
