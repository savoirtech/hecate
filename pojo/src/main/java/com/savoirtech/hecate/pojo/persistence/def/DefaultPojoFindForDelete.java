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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Lists;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.FacetMappingVisitor;
import com.savoirtech.hecate.pojo.mapping.ReferenceFacetMapping;
import com.savoirtech.hecate.pojo.mapping.ScalarFacetMapping;
import com.savoirtech.hecate.pojo.persistence.Evaporator;
import com.savoirtech.hecate.pojo.persistence.PersistenceContext;
import com.savoirtech.hecate.pojo.persistence.PojoFindForDelete;
import com.savoirtech.hecate.core.util.CqlUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class DefaultPojoFindForDelete<P> extends PojoStatement<P> implements PojoFindForDelete {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final List<ReferenceFacetMapping> facetMappings;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoFindForDelete(PersistenceContext persistenceContext, PojoMapping<P> pojoMapping) {
        super(persistenceContext, pojoMapping);
        this.facetMappings = new LinkedList<>();
        for (FacetMapping mapping : pojoMapping.getSimpleMappings()) {
            mapping.accept(new FacetMappingVisitor() {
                @Override
                public void visitReference(ReferenceFacetMapping referenceMapping) {
                    if (referenceMapping.getFacet().isCascadeDelete()) {
                        facetMappings.add(referenceMapping);
                    }
                }

                @Override
                public void visitScalar(ScalarFacetMapping scalarMapping) {

                }
            });
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoFindForDelete Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void execute(Iterable<Object> ids, Evaporator evaporator, StatementOptions options) {
        List<Object> parameters = Collections.singletonList(Lists.newArrayList(ids));
        ResultSet rows = executeStatement(parameters, options);
        for (Row row : rows) {
            Iterator<Object> columnValues = CqlUtils.toList(row).iterator();
            for (ReferenceFacetMapping mapping : facetMappings) {
                evaporator.evaporate(mapping.getElementMapping(), mapping.getColumnType().columnElements(columnValues.next()));
            }
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected RegularStatement createStatement() {
        Select.Selection select = QueryBuilder.select();
        facetMappings.forEach(mapping -> select.column(mapping.getColumnName()));
        return select
                .from(getPojoMapping().getTableName())
                .where(QueryBuilder.in(getPojoMapping().getForeignKeyMapping().getColumnName(), QueryBuilder.bindMarker()));
    }
}
