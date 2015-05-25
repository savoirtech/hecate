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

package com.savoirtech.hecate.pojo.dao.def;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.savoirtech.hecate.core.metrics.HecateMetrics;
import com.savoirtech.hecate.core.query.QueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.Evaporator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoQueryBuilder;

import java.util.Collections;

import static com.savoirtech.hecate.core.metrics.MetricsUtils.doWithTimer;
import static com.savoirtech.hecate.core.metrics.MetricsUtils.returnWithTimer;

public class DefaultPojoDao<I, P> implements PojoDao<I, P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoMapping<P> pojoMapping;
    private final PersistenceContext persistenceContext;
    private final Timer deleteTimer;
    private final Timer saveTimer;
    private final Timer findById;
    private final Timer findByIds;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(PojoMapping<P> pojoMapping, PersistenceContext persistenceContext) {
        this.pojoMapping = pojoMapping;
        this.persistenceContext = persistenceContext;
        deleteTimer = createTimer(pojoMapping,"delete");
        saveTimer = createTimer(pojoMapping,"save");
        findById = createTimer(pojoMapping,"findById");
        findByIds = createTimer(pojoMapping,"findByIds");
    }

    private static Timer createTimer(PojoMapping<?> mapping, String operation) {
        return HecateMetrics.REGISTRY.timer(MetricRegistry.name(mapping.getPojoClass().getSimpleName(), mapping.getTableName(), operation));
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDao Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void delete(I id, StatementOptions options) {
        doWithTimer(deleteTimer, () -> {
            Evaporator evaporator = persistenceContext.createEvaporator(options);
            evaporator.evaporate(pojoMapping, Collections.singleton(id));
            evaporator.execute();
        });
    }

    @Override
    public PojoQueryBuilder<P> find() {
        return persistenceContext.find(pojoMapping);
    }

    @Override
    public P findById(I id) {
        return returnWithTimer(findById, () -> persistenceContext.findById(pojoMapping).execute(id).one());
    }

    @Override
    public QueryResult<P> findByIds(Iterable<I> ids) {
        return returnWithTimer(findByIds, () -> persistenceContext.findByIds(pojoMapping).execute(ids));
    }

    @Override
    public final void save(P pojo, StatementOptions options) {
        save(pojo, pojoMapping.getTtl(), options);
    }

    @Override
    public void save(P pojo, int ttl, StatementOptions options) {
        doWithTimer(saveTimer, () -> {
            Dehydrator dehydrator = persistenceContext.createDehydrator(ttl, options);
            dehydrator.dehydrate(pojoMapping, Collections.singleton(pojo));
            dehydrator.execute();
        });
    }
}
