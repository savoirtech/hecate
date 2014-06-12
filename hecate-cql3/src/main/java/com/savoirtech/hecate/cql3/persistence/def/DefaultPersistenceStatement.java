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
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
        this.preparedStatement = persistenceContext.getSession().prepare(statement);
        this.pojoMapping = pojoMapping;
        this.parameterMappings = new ArrayList<>(parameterMappings);
        logger.info("{}: {}", pojoMapping.getPojoMetadata().getPojoType().getSimpleName(), statement);
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
        Validate.isTrue(parameters.size() == parameterMappings.size(), "Expected %d parameters, but received %d.", parameterMappings.size(), parameters.size());
        return executeStatementRaw(convertParameters(parameters));
    }

    private List<Object> convertParameters(List<Object> parameters) {
        if (parameters.isEmpty()) {
            return Collections.emptyList();
        }
        List<Object> converted = new ArrayList<>(parameters.size());
        int index = 0;
        for (Object parameter : parameters) {
            converted.add(convert(parameter, parameterMappings.get(index)));
            index++;
        }
        return converted;
    }

    @SuppressWarnings("unchecked")
    private Object convert(Object parameter, FacetMapping facetMapping) {
        if (parameter == null) {
            return null;
        }
        if (parameter.getClass().isArray()) {
            return convertArray(parameter, facetMapping);
        }
        if (List.class.isInstance(parameter)) {
            return convertCollectionParameter((List<Object>) parameter, new LinkedList<>(), facetMapping);
        }
        if (Set.class.isInstance(parameter)) {
            return convertCollectionParameter((Set<Object>) parameter, new HashSet<>(), facetMapping);
        }
        return facetMapping.getColumnHandler().convertElement(parameter);
    }

    private Object convertArray(Object parameter, FacetMapping facetMapping) {
        final int length = Array.getLength(parameter);
        Object copy = Array.newInstance(parameter.getClass().getComponentType(), length);
        for (int i = 0; i < length; ++i) {
            Array.set(copy, i, facetMapping.getColumnHandler().convertElement(Array.get(parameter, i)));
        }
        return copy;
    }

    private <T extends Collection<Object>> T convertCollectionParameter(Collection<Object> parameters, T converted, FacetMapping facetMapping) {
        for (Object parameterElement : parameters) {
            converted.add(facetMapping.getColumnHandler().convertElement(parameterElement));
        }
        return converted;
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
