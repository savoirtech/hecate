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

import com.google.common.collect.Iterables;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoInsert;

import java.util.*;

public class DefaultDehydrator implements Dehydrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final Map<PojoMapping<Object>,List<Object>> agenda = new HashMap<>();
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
        List<Object> list = agenda.get(pojoMapping);
        if(list == null) {
            list = new LinkedList<>();
            agenda.put((PojoMapping<Object>)pojoMapping, list);
        }
        Iterables.addAll(list, pojos);
    }

    public void execute() {
        while (!agenda.isEmpty()) {
            final Set<PojoMapping<Object>> pojoMappings = new HashSet<>(agenda.keySet());
            pojoMappings.forEach(mapping -> {
                List<Object> pojos = agenda.remove(mapping);
                PojoInsert<Object> insert = persistenceContext.insert(mapping);
                pojos.forEach(pojo -> insert.insert(pojo, this, ttl, options));
            });
        }
    }
}
