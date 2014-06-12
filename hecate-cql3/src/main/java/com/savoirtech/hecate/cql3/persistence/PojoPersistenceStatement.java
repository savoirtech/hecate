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

package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.util.Callback;
import com.savoirtech.hecate.cql3.util.CassandraUtils;
import com.savoirtech.hecate.cql3.value.Facet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PojoPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Session session;
    private final PreparedStatement preparedStatement;
    private final PojoMapping pojoMapping;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    protected static Select pojoSelect(PojoMapping pojoMapping) {
        final Select.Selection select = QueryBuilder.select();
        for (FacetMapping mapping : pojoMapping.getFacetMappings()) {
            select.column(mapping.getFacetMetadata().getColumnName());
        }
        return select.from(pojoMapping.getTableName());
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected PojoPersistenceStatement(Session session, RegularStatement statement, PojoMapping pojoMapping) {
        this.session = session;
        logger.info("{}: {}", pojoMapping.getPojoMetadata().getPojoType().getSimpleName(), statement);
        this.preparedStatement = session.prepare(statement);
        this.pojoMapping = pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public PojoMapping getPojoMapping() {
        return pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected List<Object> cassandraIdentifiers(Iterable<Object> keys) {
        List<Object> cassandraValues = new LinkedList<>();
        for (Object key : keys) {
            cassandraValues.add(getPojoMapping().getIdentifierMapping().getColumnHandler().convertElement(key));
        }
        return cassandraValues;
    }

    protected ResultSet executeWithArgs(Object... parameters) {
        return executeWithList(Arrays.asList(parameters));
    }

    protected ResultSet executeWithList(List<Object> parameters) {
        logger.debug("CQL: {} with parameters {}...", preparedStatement.getQueryString(), parameters);
        return session.execute(preparedStatement.bind(parameters.toArray(new Object[parameters.size()])));
    }

    protected List<Object> list(Map<Object, Object> pojoMap, ResultSet resultSet, QueryContext context) {
        List<Row> rows = resultSet.all();
        final Map<Object, Object> cassandraKeyedPojos = cassandraKeyedPojoMap(pojoMap);
        final List<Object> pojos = new ArrayList<>(rows.size());
        final FacetMapping identifierMapping = pojoMapping.getIdentifierMapping();
        for (Row row : rows) {
            Object cassandraIdentifier = CassandraUtils.getValue(row, 0, identifierMapping.getColumnHandler().getColumnType());
            Object pojo = cassandraKeyedPojos.get(cassandraIdentifier);
            if (pojo != null) {
                pojos.add(mapPojoFromRow(pojo, row, context));
            }
        }
        return pojos;
    }

    private Map<Object, Object> cassandraKeyedPojoMap(Map<Object, Object> pojoMap) {
        Map<Object, Object> cassandraKeyed = new HashMap<>();
        for (Map.Entry<Object, Object> entry : pojoMap.entrySet()) {
            cassandraKeyed.put(pojoMapping.getIdentifierMapping().getColumnHandler().convertElement(entry.getKey()), entry.getValue());
        }
        return cassandraKeyed;
    }

    @SuppressWarnings("unchecked")
    protected Object mapPojoFromRow(Object pojo, Row row, QueryContext context) {
        int columnIndex = 0;
        for (FacetMapping mapping : pojoMapping.getFacetMappings()) {
            Object columnValue = CassandraUtils.getValue(row, columnIndex, mapping.getColumnHandler().getColumnType());
            final FacetValueTarget target = new FacetValueTarget(pojo, mapping.getFacetMetadata().getFacet());
            mapping.getColumnHandler().injectFacetValue(target, columnValue, context);
            columnIndex++;
        }
        return pojo;
    }

    protected Object one(Object pojo, ResultSet resultSet, QueryContext context) {
        Row row = resultSet.one();
        return row == null ? null : mapPojoFromRow(pojo, row, context);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static final class FacetValueTarget implements Callback<Object> {
        private final Object pojo;
        private final Facet facet;

        private FacetValueTarget(Object pojo, Facet facet) {
            this.pojo = pojo;
            this.facet = facet;
        }

        @Override
        public void execute(Object value) {
            facet.set(pojo, value);
        }
    }
}
