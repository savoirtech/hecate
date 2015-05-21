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
import com.savoirtech.hecate.pojo.mapping.element.ElementInjector;
import com.savoirtech.hecate.pojo.persistence.Hydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class DefaultHydrator implements Hydrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final List<Pair<PojoMapping<?>,ElementInjector>> injectors = new LinkedList<>();
    private final Multimap<PojoMapping<?>, Object> agenda = MultimapBuilder.hashKeys().linkedListValues().build();
    private final Map<PojoMapping,Map<Object,Object>> resolved = new HashMap<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultHydrator(PersistenceContext persistenceContext) {
        this.persistenceContext = persistenceContext;
    }

//----------------------------------------------------------------------------------------------------------------------
// Hydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void execute() {
        while(!agenda.isEmpty()) {
            final Set<PojoMapping<?>> pojoMappings = new HashSet<>(agenda.keySet());
            pojoMappings.forEach(mapping -> {
                Map<Object,Object> idToPojo = getOrCreateIdToPojo(mapping);
                List<Object> ids = new ArrayList<>(agenda.removeAll(mapping));
                ids.removeAll(idToPojo.keySet());
                if(!ids.isEmpty()) {
                    List<?> pojos = persistenceContext.findByIds(mapping).execute(ids).list();
                    for (Object pojo : pojos) {
                        final Object idFacetValue = mapping.getForeignKeyMapping().getFacet().getValue(pojo);
                        final Object idCassandraValue = mapping.getForeignKeyMapping().getColumnType().convertParameterValue(idFacetValue);
                        idToPojo.put(idCassandraValue,pojo);
                    }
                }
            });
        }
        for (Pair<PojoMapping<?>, ElementInjector> injector : injectors) {
            final Map<Object,Object> idToPojo = resolved.get(injector.getLeft());
            if(idToPojo != null) {
                injector.getRight().injectElement(idToPojo::get);
            }
        }
    }

    @Override
    public void resolveElements(PojoMapping<?> pojoMapping, Iterable<Object> cassandraValues, ElementInjector injector) {
        agenda.putAll(pojoMapping, cassandraValues);
        injectors.add(new ImmutablePair<>(pojoMapping, injector));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Map<Object,Object> getOrCreateIdToPojo(PojoMapping<?> mapping) {
        Map<Object,Object> idToPojo = resolved.get(mapping);
        if(idToPojo == null) {
            idToPojo = new HashMap<>();
            resolved.put(mapping,idToPojo);
        }
        return idToPojo;
    }
}
