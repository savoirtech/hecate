/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.persistence.def;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.update.UpdateGroup;
import com.savoirtech.hecate.pojo.mapping.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoDelete;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public class DefaultPojoDelete<P> extends PojoStatement<P> implements PojoDelete<P> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDelete(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        super(persistenceContext, pojoMapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDelete Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void delete(UpdateGroup updateGroup, Iterable<Object> ids, StatementOptions options) {
        List<FacetMapping> idMappings = getPojoMapping().getIdMappings();
        if (idMappings.size() > 1) {
            for (Object compositeKey : ids) {
                final List<Object> parameters = idMappings.stream().map(mapping -> {
                    final Object facetValue = mapping.getFacet().flatten().getValue(compositeKey);
                    return mapping.getColumnValueForFacetValue(facetValue);
                }).collect(Collectors.toList());
                executeUpdate(updateGroup, parameters, options);
            }
        } else {
            final FacetMapping mapping = idMappings.get(0);
            for (Object id : ids) {
                executeUpdate(updateGroup, Collections.singletonList(mapping.getColumnValueForFacetValue(id)), options);
            }
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected RegularStatement createStatement() {
        Delete.Where delete = QueryBuilder.delete().from(getPojoMapping().getTableName()).where();
        getPojoMapping().getIdMappings().forEach(mapping -> delete.and(QueryBuilder.eq(mapping.getColumnName(), bindMarker())));
        return delete;
    }
}
