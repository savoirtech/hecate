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

import com.datastax.driver.core.Statement;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoInsert;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class DefaultDehydrator implements Dehydrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final Multimap<PojoMapping<Object>, Object> agenda = MultimapBuilder.hashKeys().linkedListValues().build();
    private final int ttl;
    private final List<Consumer<Statement>> modifiers;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultDehydrator(PersistenceContext persistenceContext, int ttl, List<Consumer<Statement>> modifiers) {
        this.persistenceContext = persistenceContext;
        this.ttl = ttl;
        this.modifiers = modifiers;
    }

//----------------------------------------------------------------------------------------------------------------------
// Dehydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public void dehydrate(PojoMapping<?> pojoMapping, Iterable<?> pojos) {
        agenda.putAll((PojoMapping<Object>)pojoMapping, pojos);
    }

    public void execute() {
        while (!agenda.isEmpty()) {
            final Set<PojoMapping<Object>> pojoMappings = new HashSet<>(agenda.keySet());
            pojoMappings.forEach(mapping -> {
                Collection<Object> pojos = agenda.removeAll(mapping);
                PojoInsert<Object> insert = persistenceContext.insert(mapping);
                pojos.forEach(pojo -> insert.insert(pojo, this, ttl, modifiers));
            });
        }
    }
}
