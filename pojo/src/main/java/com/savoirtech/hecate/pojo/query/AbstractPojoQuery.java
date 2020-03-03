/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.query;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.internal.core.cql.MultiPageResultSet;
import com.datastax.oss.driver.internal.core.cql.SinglePageResultSet;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.google.common.collect.Iterables;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.core.query.QueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.query.mapper.PojoQueryRowMapper;
import java.util.stream.Collectors;

public abstract class AbstractPojoQuery<P> implements PojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoBinding<P> binding;
    private final CqlSession session;
    private final PreparedStatement statement;
    private final PojoQueryContextFactory contextFactory;
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractPojoQuery(CqlSession session, PojoBinding<P> binding, PojoQueryContextFactory contextFactory, Select select, Executor executor) {
        this.session = session;
        this.binding = binding;
        this.executor = executor;
        this.statement = session.prepare(select.build());
        this.contextFactory = contextFactory;
    }

    public AbstractPojoQuery(CqlSession session, PojoBinding<P> binding, PojoQueryContextFactory contextFactory, PreparedStatement statement, Executor executor) {
        this.session = session;
        this.binding = binding;
        this.statement = statement;
        this.contextFactory = contextFactory;
        this.executor = executor;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object[] convertParameters(Object[] params);

//----------------------------------------------------------------------------------------------------------------------
// PojoQuery Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public QueryResult<P> execute(StatementOptions options, Object... params) {
        return multi(options).add(params).execute();
    }

    @Override
    public PojoMultiQuery<P> multi(StatementOptions options) {
        return new PojoMultiQueryImpl(options);
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    protected PojoBinding<P> getBinding() {
        return binding;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class PojoMultiQueryImpl implements PojoMultiQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final List<CompletionStage<AsyncResultSet>> futures = new LinkedList<>();
        private final StatementOptions options;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public PojoMultiQueryImpl(StatementOptions options) {
            this.options = options;
        }

//----------------------------------------------------------------------------------------------------------------------
// PojoMultiQuery Implementation
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public PojoMultiQuery<P> add(Object... params) {
            BoundStatement boundStatement = CqlUtils.bind(statement,  convertParameters(params));
            Statement optionsApplied = options.applyTo(boundStatement);
            futures.add(session.executeAsync(optionsApplied).toCompletableFuture());
            return this;
        }

        @Override
        public MappedQueryResult<P> execute() {
            try {
                CompletableFutures.getUninterruptibly(CompletableFuture.allOf(futures.stream()
                        .map(stage -> stage.toCompletableFuture())
                        .collect(Collectors.toList())
                        .toArray(new CompletableFuture[0])));

                ResultSet[] results = new ResultSet[futures.size()];
                for (CompletionStage<AsyncResultSet> stage : futures) {
                    int index = futures.indexOf(stage);
                    AsyncResultSet asyncResultSet = stage.toCompletableFuture().get();
                    if (asyncResultSet.hasMorePages()) {
                        results[index] = new MultiPageResultSet(asyncResultSet);
                    } else {
                        results[index] = new SinglePageResultSet(asyncResultSet);
                    }

                }

                Iterable<Row> rows = Iterables.concat(results);

                return new MappedQueryResult<>(rows, new PojoQueryRowMapper<>(binding, contextFactory.createPojoQueryContext(executor)));
            } catch (ExecutionException e) {
                throw new HecateException(e, "A problem occurred while executing one of the queries.");
            } catch (InterruptedException e) {
                throw new HecateException(e, "A problem occurred while executing one of the queries.");
            }
        }
    }
}
