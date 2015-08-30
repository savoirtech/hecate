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

import java.util.*;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Evaporator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;

public class DefaultEvaporator implements Evaporator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Multimap<PojoMapping<?>, Object> agenda = MultimapBuilder.hashKeys().linkedListValues().build();
    private final Multimap<PojoMapping<?>, Object> visited = MultimapBuilder.hashKeys().hashSetValues().build();
    private final PersistenceContext persistenceContext;
    private final StatementOptions options;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultEvaporator(PersistenceContext persistenceContext, StatementOptions options) {
        this.persistenceContext = persistenceContext;
        this.options = options;
    }

//----------------------------------------------------------------------------------------------------------------------
// Evaporator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void evaporate(PojoMapping<?> mapping, Iterable<Object> ids) {
        final HashSet<Object> uniqueIds = Sets.newHashSet(ids);
        Collection<Object> visitedIds = visited.get(mapping);
        if (visitedIds != null) {
            uniqueIds.removeAll(visitedIds);
        }
        visited.putAll(mapping, uniqueIds);
        agenda.putAll(mapping, uniqueIds);
    }

    @Override
    public void execute() {
        while (!agenda.isEmpty()) {
            final Set<PojoMapping<?>> pojoMappings = new HashSet<>(agenda.keySet());
            pojoMappings.forEach(mapping -> {
                Collection<Object> ids = agenda.removeAll(mapping);
                if (mapping.isCascadeDelete()) {
                    persistenceContext.findForDelete(mapping).execute(ids, DefaultEvaporator.this, options);
                }
            });
        }
        final List<ResultSetFuture> futures = new LinkedList<>();
        for (PojoMapping<?> pojoMapping : visited.keySet()) {
            persistenceContext.delete(pojoMapping).delete(futures, visited.get(pojoMapping), options);
        }
        try {
            Uninterruptibles.getUninterruptibly(Futures.allAsList(futures));
        } catch (ExecutionException e) {
            throw new HecateException(e, "An error occurred while waiting for delete statements to finish.");
        }
    }
}
