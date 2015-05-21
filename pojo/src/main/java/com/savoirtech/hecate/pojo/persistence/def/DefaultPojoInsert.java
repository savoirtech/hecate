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
import com.savoirtech.hecate.pojo.mapping.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoInsert;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class DefaultPojoInsert<P> extends PojoStatement<P> implements PojoInsert<P> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoInsert(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        super(persistenceContext, pojoMapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected RegularStatement createStatement() {
        return getPojoMapping().createInsertStatement();
    }

    @Override
    public void insert(P pojo, Dehydrator dehydrator, Consumer<Statement>... modifiers) {
        List<Object> parameters = new LinkedList<>();
        collectParameters(parameters, pojo, dehydrator, getPojoMapping().getIdMappings());
        collectParameters(parameters, pojo, dehydrator, getPojoMapping().getSimpleMappings());
        executeStatementRaw(parameters, modifiers);
    }

    private void collectParameters(List<Object> parameters, P pojo, Dehydrator dehydrator, List<FacetMapping> mappings) {
        mappings.forEach(mapping -> parameters.add(mapping.getColumnType().getInsertValue(dehydrator, mapping.getFacet().getValue(pojo))));
    }
}
