/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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
import com.savoirtech.hecate.cql3.persistence.Evaporator;
import com.savoirtech.hecate.cql3.persistence.PojoFindForDelete;
import com.savoirtech.hecate.cql3.util.HecateUtils;

import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class DefaultPojoFindForDelete extends DefaultPersistenceStatement implements PojoFindForDelete {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------
    public DefaultPojoFindForDelete(DefaultPersistenceContext persistenceContext, PojoMapping mapping) {
        super(persistenceContext, createSelect(mapping), mapping, mapping.getIdentifierMapping());
    }

    private static Select.Where createSelect(PojoMapping pojoMapping) {
        final Select.Selection select = select();
        for (FacetMapping mapping : pojoMapping.getFacetMappings()) {
            if (mapping.getColumnHandler().isCascading()) {
                select.column(mapping.getFacetMetadata().getColumnName());
            }
        }
        return select.from(pojoMapping.getTableName()).where(in(pojoMapping.getIdentifierMapping().getFacetMetadata().getColumnName(), bindMarker()));
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoFindForDelete Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void execute(Iterable<Object> keys, Evaporator evaporator) {
        final List<Object> keyList = toList(keys);
        if (!keyList.isEmpty()) {
            getLogger().info("Looking for keys to delete...");
            final ResultSet resultSet = executeStatementArgs(keyList).getUninterruptibly();
            for (Row row : resultSet) {
                processRow(row, evaporator);
            }
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void processRow(Row row, Evaporator evaporator) {
        int columnIndex = 0;
        for (FacetMapping mapping : getPojoMapping().getFacetMappings()) {
            if (mapping.getColumnHandler().isCascading()) {
                Object columnValue = HecateUtils.getValue(row, columnIndex, mapping.getColumnHandler().getColumnType());
                mapping.getColumnHandler().getDeletionIdentifiers(columnValue, evaporator);
                columnIndex++;
            }
        }
    }
}
