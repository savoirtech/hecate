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

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.facet.ScalarFacetMapping;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoDelete;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;

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
    public void delete(Iterable<Object> ids, List<Consumer<Statement>> modifiers) {
        List<ScalarFacetMapping> idMappings = getPojoMapping().getIdMappings();
        if(idMappings.size() > 1) {
            for (Object compositeKey : ids) {
                final List<Object> parameters = idMappings.stream().map(mapping -> mapping.getColumnValueForFacetValue(compositeKey)).collect(Collectors.toList());
                executeStatement(parameters, modifiers);
            }
        }
        else {
            final ScalarFacetMapping mapping = idMappings.get(0);
            executeStatement(Collections.singletonList(Lists.newLinkedList(ids).stream().map(mapping::getColumnValueForFacetValue).collect(Collectors.toList())), modifiers);
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected RegularStatement createStatement() {
        Delete.Where delete = QueryBuilder.delete().from(getPojoMapping().getTableName()).where();
        if(getPojoMapping().getIdMappings().size() > 1) {
            getPojoMapping().getIdMappings().forEach(mapping -> delete.and(QueryBuilder.eq(mapping.getFacet().getColumnName(), bindMarker())));
        }
        else {
            getPojoMapping().getIdMappings().forEach(mapping -> delete.and(in(mapping.getFacet().getColumnName(),bindMarker())));
        }
        return delete;
    }
}
