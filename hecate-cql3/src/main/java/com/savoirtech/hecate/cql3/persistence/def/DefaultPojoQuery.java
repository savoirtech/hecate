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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Hydrator;
import com.savoirtech.hecate.cql3.persistence.PojoQuery;
import com.savoirtech.hecate.cql3.persistence.PojoQueryResult;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class DefaultPojoQuery<P> implements PojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final DefaultPersistenceContext persistenceContext;
    private final PreparedStatement preparedStatement;
    private final FacetMapping[] parameterMappings;
    private final PojoMapping pojoMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoQuery(DefaultPersistenceContext persistenceContext, Select.Where statement, PojoMapping pojoMapping, List<FacetMapping> parameterMappings) {
        this.persistenceContext = persistenceContext;
        this.pojoMapping = pojoMapping;
        this.preparedStatement = persistenceContext.getSession().prepare(statement);
        this.parameterMappings = parameterMappings.toArray(new FacetMapping[parameterMappings.size()]);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoQuery Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoQueryResult<P> execute(Object... parameters) {
        return execute(persistenceContext.newHydrator(), parameters);
    }

    public PojoQueryResult<P> execute(Hydrator hydrator, Object... parameters) {
        Validate.isTrue(parameters.length == parameterMappings.length, "Expected %d parameters, but received %d.", parameterMappings.length, parameters.length);
        return new PojoQueryResultImpl(persistenceContext.getSession().execute(preparedStatement.bind(convertParameters(parameters))), hydrator);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Object[] convertParameters(Object... parameters) {
        if (ArrayUtils.isEmpty(parameters)) {
            return ArrayUtils.nullToEmpty(parameters);
        }

        Object[] converted = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            Object parameter = parameters[i];
            converted[i] = convert(parameter, parameterMappings[i]);
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

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private final class HydratorIterator implements Iterator<P> {
        private final Iterator<Row> rows;
        private final Hydrator hydrator;

        private HydratorIterator(Iterator<Row> rows, Hydrator hydrator) {
            this.rows = rows;
            this.hydrator = hydrator;
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public P next() {
            return hydrator.hydrate(pojoMapping, rows.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot 'remove' objects from a query result.");
        }
    }

    private final class PojoQueryResultImpl implements PojoQueryResult<P> {
        private final ResultSet resultSet;
        private final Hydrator hydrator;

        private PojoQueryResultImpl(ResultSet resultSet, Hydrator hydrator) {
            this.resultSet = resultSet;
            this.hydrator = hydrator;
        }

        @Override
        public Iterator<P> iterate() {
            return new HydratorIterator(resultSet.iterator(), hydrator);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<P> list() {
            List<P> results = new LinkedList<>();
            for (Row row : resultSet) {
                results.add((P) hydrator.hydrate(pojoMapping, row));
            }
            return results;
        }

        @Override
        @SuppressWarnings("unchecked")
        public P one() {
            return (P) hydrator.hydrate(pojoMapping, resultSet.one());
        }
    }
}
