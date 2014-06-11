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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoFindByKey extends PojoPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindByKey.class);

    private final PojoMapping pojoMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoFindByKey(Session session, PojoMapping mapping) {
        super(session, createSelect(mapping), mapping);
        this.pojoMapping = mapping;
    }

    private static Select.Where createSelect(PojoMapping mapping) {
        final Select.Where where = pojoSelect(mapping)
                .where(QueryBuilder.eq(mapping.getIdentifierMapping().getFacetMetadata().getColumnName(), QueryBuilder.bindMarker()));

        LOGGER.info("{}.findByKey(): {}", mapping.getPojoMetadata().getPojoType().getSimpleName(), where);
        return where;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public Object find(Object identifier, QueryContext context) {
        return find(identifier, pojoMapping.getPojoMetadata().newPojo(identifier), context);
    }

    public Object find(Object identifier, Object pojo, QueryContext context) {
        return one(pojo, executeWithArgs(pojoMapping.getIdentifierMapping().getColumnHandler().convertElement(identifier)), context);
    }
}
