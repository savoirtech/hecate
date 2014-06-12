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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.util.HecateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DefaultPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final DefaultPersistenceContext persistenceContext;
    private final PreparedStatement preparedStatement;
    private final PojoMapping pojoMapping;
    private final List<FacetMapping> parameterMappings;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected DefaultPersistenceStatement(DefaultPersistenceContext persistenceContext, RegularStatement statement, PojoMapping pojoMapping, List<FacetMapping> parameterMappings) {
        this.persistenceContext = persistenceContext;
        logger.info("{}: {}", pojoMapping.getPojoMetadata().getPojoType().getSimpleName(), statement);
        this.preparedStatement = persistenceContext.getSession().prepare(statement);
        this.pojoMapping = pojoMapping;
        this.parameterMappings = new ArrayList<>(parameterMappings);
    }

    protected DefaultPersistenceStatement(DefaultPersistenceContext persistenceContext, RegularStatement statement, PojoMapping pojoMapping, FacetMapping... parameterMappings) {
        this(persistenceContext, statement, pojoMapping, Arrays.asList(parameterMappings));
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    protected DefaultPersistenceContext getPersistenceContext() {
        return persistenceContext;
    }

    protected PojoMapping getPojoMapping() {
        return pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected ResultSet executeStatementArgs(Object... parameters) {
        List<Object> parameterList = new ArrayList<>(parameters.length);
        for (Object parameter : parameters) {
            parameterList.add(parameter);
        }
        return executeStatementList(parameterList);
    }

    protected ResultSet executeStatementList(List<Object> parameters) {
        return executeStatementRaw(HecateUtils.convertParameters(parameters, parameterMappings));
    }

    protected ResultSet executeStatementRaw(List<Object> parameters) {
        logger.debug("CQL: {} with parameters {}", preparedStatement.getQueryString(), parameters);
        BoundStatement boundStatement = preparedStatement.bind(parameters.toArray(new Object[parameters.size()]));
        return persistenceContext.getSession().execute(boundStatement);
    }

    protected <T> List<T> toList(Iterable<T> iterable) {
        List<T> list = new LinkedList<>();
        for (T element : iterable) {
            list.add(element);
        }
        return list;
    }
}
