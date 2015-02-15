/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.PojoSave;

import java.util.ArrayList;
import java.util.List;

public class DefaultPojoSave extends DefaultPersistenceStatement implements PojoSave {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoSave(DefaultPersistenceContext persistenceContext, PojoMapping pojoMapping) {
        super(persistenceContext, createInsert(pojoMapping), pojoMapping, pojoMapping.getFacetMappings());
    }

    private static Insert createInsert(PojoMapping mapping) {
        final Insert insert = QueryBuilder.insertInto(mapping.getTableName());
        for (FacetMapping facetMapping : mapping.getFacetMappings()) {
            insert.value(facetMapping.getFacetMetadata().getColumnName(), QueryBuilder.bindMarker());
        }
        insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
        return insert;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoSave Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void execute(Object pojo) {
        execute(getPersistenceContext().newDehydrator(), pojo).getUninterruptibly();
    }

    @Override
    public ListenableFuture<Void> executeAsync(Object pojo) {
        return Futures.transform(execute(getPersistenceContext().newDehydrator(), pojo), toVoid());
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    ResultSetFuture execute(Dehydrator dehydrator, Object pojo) {
        List<Object> parameters = new ArrayList<>(getPojoMapping().getFacetMappings().size() + 1);
        for (FacetMapping mapping : getPojoMapping().getFacetMappings()) {
            final Object facetValue = mapping.getFacetMetadata().getFacet().get(pojo);
            parameters.add(mapping.getColumnHandler().getInsertValue(facetValue, dehydrator));
        }
        if (dehydrator.hasGlobalTtl()) {
            parameters.add(dehydrator.getTtl());
        } else {
            parameters.add(getPojoMapping().getPojoMetadata().getTimeToLive());
        }
        return executeStatementRaw(parameters);
    }
}
