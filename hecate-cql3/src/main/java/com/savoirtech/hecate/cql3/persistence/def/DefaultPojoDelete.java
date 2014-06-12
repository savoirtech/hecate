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

import com.datastax.driver.core.querybuilder.Delete;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Evaporator;
import com.savoirtech.hecate.cql3.persistence.PojoDelete;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class DefaultPojoDelete extends DefaultPersistenceStatement implements PojoDelete {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDelete(DefaultPersistenceContext persistenceContext, PojoMapping mapping) {
        super(persistenceContext, createDelete(mapping), mapping, mapping.getIdentifierMapping());
    }

    private static Delete.Where createDelete(PojoMapping mapping) {
        return delete().from(mapping.getTableName()).where(in(mapping.getIdentifierMapping().getFacetMetadata().getColumnName(), bindMarker()));
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDelete Implementation
//----------------------------------------------------------------------------------------------------------------------

    public void execute(Iterable<Object> keys) {
        execute(getPersistenceContext().newEvaporator(), keys);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void execute(Evaporator evaporator, Iterable<Object> keys) {
        getPersistenceContext().findForDelete(getPojoMapping().getPojoMetadata().getPojoType(), getPojoMapping().getTableName()).execute(keys, evaporator);
        executeStatementArgs(toList(keys));
    }
}
