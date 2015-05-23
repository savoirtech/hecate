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
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.FacetMappingVisitor;
import com.savoirtech.hecate.pojo.mapping.ReferenceFacetMapping;
import com.savoirtech.hecate.pojo.mapping.ScalarFacetMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoInsert;

import java.util.LinkedList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

public class DefaultPojoInsert<P> extends PojoStatement<P> implements PojoInsert<P> {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoInsert(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        super(persistenceContext, pojoMapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoInsert Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void insert(P pojo, Dehydrator dehydrator, int ttl, StatementOptions options) {
        List<Object> parameters = new LinkedList<>();
        collectParameters(parameters, pojo, dehydrator, getPojoMapping().getIdMappings());
        collectParameters(parameters, pojo, dehydrator, getPojoMapping().getSimpleMappings());
        parameters.add(ttl);
        executeStatement(parameters, options);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private void collectParameters(List<Object> parameters, P pojo, Dehydrator dehydrator, List<? extends FacetMapping> mappings) {
        mappings.forEach(mapping -> mapping.accept(new InsertVisitor(parameters, pojo, dehydrator)));
    }

    @Override
    protected RegularStatement createStatement() {
        Insert insert = insertInto(getPojoMapping().getTableName());
        getPojoMapping().getIdMappings().forEach(mapping -> insert.value(mapping.getFacet().getColumnName(), bindMarker()));
        getPojoMapping().getSimpleMappings().forEach(mapping -> insert.value(mapping.getFacet().getColumnName(), bindMarker()));
        insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
        return insert;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class InsertVisitor implements FacetMappingVisitor {
        private final List<Object> parameters;
        private final P pojo;
        private final Dehydrator dehydrator;

        public InsertVisitor(List<Object> parameters, P pojo, Dehydrator dehydrator) {
            this.parameters = parameters;
            this.pojo = pojo;
            this.dehydrator = dehydrator;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void visitReference(ReferenceFacetMapping referenceMapping) {
            Object columnValue = referenceMapping.getColumnValue(pojo);
            parameters.add(columnValue);
            if (referenceMapping.getFacet().isCascadeSave()) {
                dehydrator.dehydrate(referenceMapping.getElementMapping(), referenceMapping.getReferences(pojo));
            }
        }

        @Override
        public void visitScalar(ScalarFacetMapping scalarMapping) {
            parameters.add(scalarMapping.getColumnValue(pojo));
        }
    }
}
