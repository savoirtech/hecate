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
import com.savoirtech.hecate.pojo.persistence.Hydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Consumer;

public class DefaultHydrator implements Hydrator {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersistenceContext persistenceContext;
    private final List<Pair<PojoMapping<?>, Consumer<Hydrator>>> callbacks = new LinkedList<>();
    private final Map<PojoMapping<Object>,List<Object>> agenda = new HashMap<>();
    private final Map<PojoMapping, Map<Object, Object>> resolved = new HashMap<>();
    private final StatementOptions options;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultHydrator(PersistenceContext persistenceContext, StatementOptions options) {
        this.persistenceContext = persistenceContext;
        this.options = options;
    }

//----------------------------------------------------------------------------------------------------------------------
// Hydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void execute() {
        while (!agenda.isEmpty()) {
            final Set<PojoMapping<Object>> pojoMappings = new HashSet<>(agenda.keySet());
            pojoMappings.forEach(mapping -> {
                Map<Object, Object> idToPojo = getOrCreateIdToPojo(mapping);
                List<Object> ids = agenda.remove(mapping);
                ids.removeAll(idToPojo.keySet());
                if (!ids.isEmpty()) {
                    List<?> pojos = persistenceContext.findByIds(mapping).execute(options, ids).list();
                    for (Object pojo : pojos) {
                        idToPojo.put(mapping.getForeignKeyMapping().getColumnValue(pojo), pojo);
                    }
                }
            });
        }
        for (Pair<PojoMapping<?>, Consumer<Hydrator>> injector : callbacks) {
            final Map<Object, Object> idToPojo = resolved.get(injector.getLeft());
            if (idToPojo != null) {
                injector.getRight().accept(this);
            }
        }
    }

    @Override
    public Object getPojo(PojoMapping<?> mapping, Object id) {
        final Map<Object, Object> idToPojo = resolved.get(mapping);
        return idToPojo == null ? null : idToPojo.get(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void resolveElements(PojoMapping<?> pojoMapping, Iterable<Object> cassandraValues, Consumer<Hydrator> callback) {
        List<Object> list = agenda.get(pojoMapping);
        if(list == null) {
            list = new LinkedList<>();
            agenda.put((PojoMapping<Object>)pojoMapping,list);
        }
        Iterables.addAll(list, cassandraValues);
        callbacks.add(new ImmutablePair<>(pojoMapping, callback));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Map<Object, Object> getOrCreateIdToPojo(PojoMapping<?> mapping) {
        Map<Object, Object> idToPojo = resolved.get(mapping);
        if (idToPojo == null) {
            idToPojo = new HashMap<>();
            resolved.put(mapping, idToPojo);
        }
        return idToPojo;
    }
}
