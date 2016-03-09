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

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.cache.PojoCache;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Hydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class DefaultHydrator implements Hydrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final List<Pair<PojoMapping<?>, Consumer<Hydrator>>> callbacks = new LinkedList<>();
    private final Multimap<PojoMapping<? extends Object>, Object> agenda = MultimapBuilder.hashKeys().hashSetValues().build();
    private final PojoCache pojoCache;
    private final StatementOptions options;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultHydrator(PersistenceContext persistenceContext, StatementOptions options, PojoCache pojoCache) {
        this.persistenceContext = persistenceContext;
        this.options = options;
        this.pojoCache = pojoCache;
    }

//----------------------------------------------------------------------------------------------------------------------
// Hydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public void execute() {
        while (!agenda.isEmpty()) {
            PojoMapping<Object> mapping = (PojoMapping<Object>) agenda.keySet().iterator().next();
            List<Object> ids = new LinkedList<>(agenda.removeAll(mapping));
            ids.removeAll(pojoCache.idSet(mapping));
            if (!ids.isEmpty()) {
                List<Object> pojos = persistenceContext.findByIds(mapping).execute(this, options, ids).list();
                pojoCache.putAll(mapping, pojos);
            }
        }
        for (Pair<PojoMapping<?>, Consumer<Hydrator>> injector : callbacks) {
            if (pojoCache.contains(injector.getLeft())) {
                injector.getRight().accept(this);
            }
        }
    }

    @Override
    public Object getPojo(PojoMapping<?> mapping, Object id) {
        return pojoCache.lookup(mapping, id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void resolveElements(PojoMapping<?> pojoMapping, Iterable<Object> cassandraValues, Consumer<Hydrator> callback) {

            agenda.putAll(pojoMapping, cassandraValues);
            callbacks.add(new ImmutablePair<>(pojoMapping, callback));


    }
}
