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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.IPojoSave;
import com.savoirtech.hecate.cql3.persistence.PojoPersistenceStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DefaultPojoSave extends PojoPersistenceStatement implements IPojoSave {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPojoSave.class);

    private final DefaultPersistenceContext persistenceContext;

    public DefaultPojoSave(DefaultPersistenceContext persistenceContext, Session session, PojoMapping pojoMapping) {
        super(session, createInsert(pojoMapping), pojoMapping);
        this.persistenceContext = persistenceContext;
    }

    private static Insert createInsert(PojoMapping mapping) {
        final Insert insert = QueryBuilder.insertInto(mapping.getTableName());
        for (FacetMapping facetMapping : mapping.getFacetMappings()) {
            insert.value(facetMapping.getFacetMetadata().getColumnName(), QueryBuilder.bindMarker());
        }
        insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
        LOGGER.info("{}.save(): {}", mapping.getPojoMetadata().getPojoType().getSimpleName(), insert);
        return insert;
    }


    @Override
    public void execute(Object pojo) {
        execute(persistenceContext.newDehydrator(), pojo);
    }

    void execute(Dehydrator dehydrator, Object pojo) {
        List<Object> parameters = new ArrayList<>(getPojoMapping().getFacetMappings().size());
        for (FacetMapping mapping : getPojoMapping().getFacetMappings()) {
            Object facetValue = mapping.getFacetMetadata().getFacet().get(pojo);
            parameters.add(mapping.getColumnHandler().getInsertValue(facetValue, dehydrator));
        }
        parameters.add(getPojoMapping().getPojoMetadata().getDefaultTtl());
        executeWithList(parameters);
    }
}
