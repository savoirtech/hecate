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
import com.google.common.collect.Multimaps;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoInsert;

import java.util.*;
import java.util.function.Consumer;

public class DefaultDehydrator implements Dehydrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final Multimap<PojoMapping<?>,Object> agenda = Multimaps.newListMultimap(new HashMap<>(), LinkedList::new);

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultDehydrator(PersistenceContext persistenceContext) {
        this.persistenceContext = persistenceContext;
    }

//----------------------------------------------------------------------------------------------------------------------
// Dehydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SafeVarargs
    public final <P> void dehydrate(PojoMapping<P> pojoMapping, P... pojos) {
        agenda.putAll(pojoMapping, Arrays.asList(pojos));
    }

    @SuppressWarnings("unchecked")
    public void execute() {
        while(!agenda.isEmpty()) {
            final Set<PojoMapping<?>> pojoMappings = new HashSet<>(agenda.keySet());
            pojoMappings.forEach(mapping -> {
                Collection<Object> pojos = agenda.removeAll(mapping);
                insertPojos((PojoMapping<Object>) mapping, pojos);
            });
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private <P> void insertPojos(PojoMapping<P> mapping, Collection<P> pojos, Consumer<Statement>... modifiers) {
        PojoInsert<P> insert = persistenceContext.insert(mapping);
        for (P pojo : pojos) {
            insert.insert(pojo, this, modifiers);
        }
    }
}
