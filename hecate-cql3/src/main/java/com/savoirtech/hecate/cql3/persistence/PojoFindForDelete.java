/*
 * Copyright (c) 2014. Savoir Technologies
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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PojoFindForDelete extends PojoPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindForDelete.class);

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoFindForDelete(Session session, PojoMapping mapping) {
        super(session, createSelect(mapping), mapping);
    }

    private static Select.Where createSelect(PojoMapping pojoMapping) {
        final Select.Selection select = select();
        for (FacetMapping mapping : pojoMapping.getFacetMappings()) {
            if (mapping.getColumnHandler().isCascading()) {
                select.column(mapping.getFacetMetadata().getColumnName());
            }
        }
        final Select.Where where = select.from(pojoMapping.getTableName()).where(in(pojoMapping.getIdentifierMapping().getFacetMetadata().getColumnName(), bindMarker()));
        LOGGER.info("{}.findForDelete(): {}", pojoMapping.getPojoMetadata().getPojoType().getSimpleName(), where);
        return where;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void execute(Iterable<Object> keys, DeleteContext context) {
        final List<Object> identifiers = cassandraIdentifiers(keys);
        if (!identifiers.isEmpty()) {
            final ResultSet resultSet = executeWithArgs(identifiers);
            for (Row row : resultSet) {
                processRow(row, context);
            }
        }
    }

    protected void processRow(Row row, DeleteContext context) {
        int columnIndex = 0;
        for (FacetMapping mapping : getPojoMapping().getFacetMappings()) {
            if (mapping.getColumnHandler().isCascading()) {
                Object columnValue = CassandraUtils.getValue(row, columnIndex, mapping.getColumnHandler().getColumnType());
                mapping.getColumnHandler().getDeletionIdentifiers(columnValue, context);
                columnIndex++;
            }
        }
    }
}
