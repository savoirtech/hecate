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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Hydrator;
import com.savoirtech.hecate.cql3.persistence.PojoQuery;
import com.savoirtech.hecate.cql3.persistence.PojoQueryResult;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class DefaultPojoQuery<P> extends DefaultPersistenceStatement implements PojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------


    public DefaultPojoQuery(DefaultPersistenceContext persistenceContext, Select.Where statement, PojoMapping pojoMapping, List<FacetMapping> parameterMappings) {
        super(persistenceContext, statement, pojoMapping, parameterMappings);
    }

    public DefaultPojoQuery(DefaultPersistenceContext persistenceContext, Select.Where statement, PojoMapping pojoMapping, List<InjectedParameter> injectedParameters, List<FacetMapping> parameterMappings) {
        super(persistenceContext, statement, pojoMapping, injectedParameters, parameterMappings);
    }



//----------------------------------------------------------------------------------------------------------------------
// PojoQuery Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoQueryResult<P> execute(Object... parameters) {
        return execute(getPersistenceContext().newHydrator(), parameters);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public PojoQueryResult<P> execute(Hydrator hydrator, Object... parameters) {
        final ResultSet resultSet = executeStatementList(Arrays.asList(parameters));
        return new PojoQueryResultImpl(resultSet, hydrator);
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
            return hydrator.hydrate(getPojoMapping(), rows.next());
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
                results.add((P) hydrator.hydrate(getPojoMapping(), row));
            }
            return results;
        }

        @Override
        @SuppressWarnings("unchecked")
        public P one() {
            final Row row = resultSet.one();
            return row == null ? null : (P) hydrator.hydrate(getPojoMapping(), row);
        }
    }
}
