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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
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

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract Object[] convertParameters(Object[] params);

//----------------------------------------------------------------------------------------------------------------------
// PojoQuery Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public MappedQueryResult<P> execute(StatementOptions options, Object... params) {
        BoundStatement boundStatement = CqlUtils.bind(statement, convertParameters(params));
        options.applyTo(boundStatement);
        ResultSet resultSet = session.execute(boundStatement);
        return new MappedQueryResult<>(resultSet, new PojoQueryRowMapper<>(binding, contextFactory.createPojoQueryContext()));
    }
}
