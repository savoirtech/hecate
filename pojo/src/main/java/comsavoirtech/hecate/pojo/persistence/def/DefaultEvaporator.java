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

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Evaporator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class DefaultEvaporator implements Evaporator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Multimap<PojoMapping<?>, Object> agenda = MultimapBuilder.hashKeys().linkedListValues().build();
    private final PersistenceContext persistenceContext;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultEvaporator(PersistenceContext persistenceContext) {
        this.persistenceContext = persistenceContext;
    }

//----------------------------------------------------------------------------------------------------------------------
// Evaporator Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public void evaporate(PojoMapping<?> mapping, Iterable<Object> ids) {
        agenda.putAll(mapping, ids);
    }

    @Override
    public void execute() {
        while(!agenda.isEmpty()) {
            final Set<PojoMapping<?>> pojoMappings = new HashSet<>(agenda.keySet());
            pojoMappings.forEach(mapping -> {
                Collection<Object> ids = agenda.removeAll(mapping);
                if(mapping.isCascadeDelete()) {
                }
            });
        }
    }
}
