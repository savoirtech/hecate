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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.query.mapper.PojoQueryRowMapper;

public abstract class AbstractPojoQuery<P> implements PojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoBinding<P> binding;
    private final Session session;
    private final PreparedStatement statement;
    private PojoQueryContextFactory contextFactory;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractPojoQuery(Session session, PojoBinding<P> binding, PojoQueryContextFactory contextFactory, Select.Where select) {
        this.session = session;
        this.binding = binding;
        this.statement = session.prepare(select);
        this.contextFactory = contextFactory;
    }

    public AbstractPojoQuery(Session session, PojoBinding<P> binding, PojoQueryContextFactory contextFactory, PreparedStatement statement) {
        this.session = session;
        this.binding = binding;
        this.statement = statement;
        this.contextFactory = contextFactory;
    }

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object[] convertParameters(Object[] params);

//----------------------------------------------------------------------------------------------------------------------
// PojoQuery Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public MappedQueryResult<P> execute(StatementOptions options, Object... params) {
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

        private final List<ResultSetFuture> futures = new LinkedList<>();
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
            BoundStatement boundStatement = CqlUtils.bind(statement, convertParameters(params));
            options.applyTo(boundStatement);
            futures.add(session.executeAsync(boundStatement));
            return this;
        }

        @Override
        public MappedQueryResult<P> execute() {
            try {
                Iterable<Row> rows = Iterables.concat(Uninterruptibles.getUninterruptibly(Futures.allAsList(futures)));
                return new MappedQueryResult<P>(rows, new PojoQueryRowMapper<>(binding, contextFactory.createPojoQueryContext()));
            } catch (ExecutionException e) {
                throw new HecateException("A problem occurred while executing one of the queries.", e);
            }
        }
    }
}
