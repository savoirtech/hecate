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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.row.HydratorRowMapper;
import com.savoirtech.hecate.pojo.persistence.Hydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoFindByIds;

public class DefaultPojoFindByIds<P> extends PojoStatement<P> implements PojoFindByIds<P> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoFindByIds(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        super(persistenceContext, pojoMapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoFindByIds Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public MappedQueryResult<P> execute(Hydrator hydrator, StatementOptions options, Iterable<?> ids) {
        try {
            List<ResultSetFuture> futures = new LinkedList<>();
            ids.forEach(id -> futures.add(executeStatementAsync(Collections.singletonList(id),options)));
            ListenableFuture<List<ResultSet>> future = Futures.allAsList(futures);
            List<ResultSet> resultSets = Uninterruptibles.getUninterruptibly(future);
            Iterable<Row> rows = Iterables.concat(resultSets);
            return new MappedQueryResult<>(rows, new HydratorRowMapper<>(getPojoMapping(), hydrator));
        } catch (ExecutionException e) {
            throw new HecateException(e, "An error occurred while waiting for all id queries to return.");
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected RegularStatement createStatement() {
        return DefaultPojoQueryBuilder.createSelect(getPojoMapping()).and(QueryBuilder.eq(getPojoMapping().getForeignKeyMapping().getColumnName(), QueryBuilder.bindMarker()));
    }
}
