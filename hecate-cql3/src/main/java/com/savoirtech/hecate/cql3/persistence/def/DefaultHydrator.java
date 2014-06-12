/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.persistence.def;

import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.handler.ColumnHandler;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Hydrator;
import com.savoirtech.hecate.cql3.util.Callback;
import com.savoirtech.hecate.cql3.util.HecateUtils;
import com.savoirtech.hecate.cql3.value.Facet;

import java.util.List;

public class DefaultHydrator extends PersistenceTaskExecutor implements Hydrator {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultHydrator(DefaultPersistenceContext persistenceContext) {
        super(persistenceContext);
    }

//----------------------------------------------------------------------------------------------------------------------
// Hydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <P> P hydrate(PojoMapping pojoMappinge, Row row) {
        final ColumnHandler<Object, Object> identifierHandler = pojoMappinge.getIdentifierMapping().getColumnHandler();
        Object identifierColumnValue = HecateUtils.getValue(row, 0, identifierHandler.getColumnType());
        Object pojo = newPojo(pojoMappinge.getPojoMetadata().getPojoType(), identifierHandler.convertIdentifier(identifierColumnValue));
        int columnIndex = 0;
        for (FacetMapping facetMapping : pojoMappinge.getFacetMappings()) {
            Object columnValue = HecateUtils.getValue(row, columnIndex, facetMapping.getColumnHandler().getColumnType());
            facetMapping.getColumnHandler().injectFacetValue(new SetFacetCallback(pojo, facetMapping.getFacetMetadata().getFacet()), columnValue, this);
            columnIndex++;
        }
        executeTasks();
        return (P) pojo;
    }

    @Override
    public void hydrate(Class<?> pojoType, String tableName, Iterable<Object> identifiers, Callback<List<Object>> callback) {
        final List<Object> prunedIdentifiers = pruneIdentifiers(pojoType, tableName, identifiers);
        if (!prunedIdentifiers.isEmpty()) {
            enqueue(new HydratePojosTask(pojoType, tableName, prunedIdentifiers, callback));
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private final class HydratePojosTask implements PersistenceTask {
        private final Class<?> pojoType;
        private final String tableName;
        private final Iterable<Object> identifiers;
        private final Callback<List<Object>> target;

        private HydratePojosTask(Class<?> pojoType, String tableName, Iterable<Object> identifiers, Callback<List<Object>> target) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.identifiers = identifiers;
            this.target = target;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void execute(DefaultPersistenceContext persistenceContext) {
            final List<Object> pojos = (List<Object>) persistenceContext.findByKeys(pojoType, tableName).execute(DefaultHydrator.this, identifiers).list();
            target.execute(pojos);
        }
    }

    private static final class SetFacetCallback implements Callback<Object> {
        private final Object pojo;
        private final Facet facet;

        private SetFacetCallback(Object pojo, Facet facet) {
            this.pojo = pojo;
            this.facet = facet;
        }

        @Override
        public void execute(Object value) {
            facet.set(pojo, value);
        }
    }
}
