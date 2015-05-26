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
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.row.HydratorRowMapper;
import com.savoirtech.hecate.pojo.persistence.Hydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoFindByIds;

import java.util.Collections;

public class DefaultPojoFindByIds<P> extends PojoStatement<P> implements PojoFindByIds<P> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoFindByIds(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        super(persistenceContext, pojoMapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoFindByIds Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public MappedQueryResult<P> execute(Hydrator hydrator, StatementOptions options, Iterable<? extends Object> ids) {
        HydratorRowMapper<P> mapper = new HydratorRowMapper<>(getPojoMapping(), hydrator);
        return new MappedQueryResult<>(executeStatement(Collections.singletonList(Lists.newArrayList(ids)), options), mapper);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected RegularStatement createStatement() {
        return DefaultPojoQueryBuilder.createSelect(getPojoMapping()).and(QueryBuilder.in(getPojoMapping().getForeignKeyMapping().getColumnName(), QueryBuilder.bindMarker()));
    }
}
