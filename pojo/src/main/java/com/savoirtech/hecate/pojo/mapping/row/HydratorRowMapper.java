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

package com.savoirtech.hecate.pojo.mapping.row;

import com.datastax.driver.core.Row;
import com.savoirtech.hecate.core.mapping.RowMapper;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.FacetMappingVisitor;
import com.savoirtech.hecate.pojo.mapping.ReferenceFacetMapping;
import com.savoirtech.hecate.pojo.mapping.ScalarFacetMapping;
import com.savoirtech.hecate.pojo.persistence.Hydrator;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.util.PojoUtils;

import java.util.Iterator;
import java.util.List;

public class HydratorRowMapper<P> implements RowMapper<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoMapping<P> mapping;
    private final Hydrator hydrator;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public HydratorRowMapper(PojoMapping<P> mapping, Hydrator hydrator) {
        this.mapping = mapping;
        this.hydrator = hydrator;
    }

//----------------------------------------------------------------------------------------------------------------------
// RowMapper Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public P map(Row row) {
        P pojo = PojoUtils.newPojo(mapping.getPojoClass());
        FacetMappingVisitor visitor = createVisitor(row, pojo);
        mapping.getIdMappings().forEach(mapping -> mapping.accept(visitor));
        mapping.getSimpleMappings().forEach(mapping -> mapping.accept(visitor));
        return pojo;
    }

    @Override
    public void mappingComplete() {
        hydrator.execute();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected FacetMappingVisitor createVisitor(Row row, final P pojo) {
        List<Object> columns = CqlUtils.toList(row);
        Iterator<Object> columnValues = columns.iterator();
        return new RowVisitor(columnValues, pojo);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class RowVisitor implements FacetMappingVisitor {
        private final Iterator<Object> columnValues;
        private final P pojo;

        public RowVisitor(Iterator<Object> columnValues, P pojo) {
            this.columnValues = columnValues;
            this.pojo = pojo;
        }

        @Override
        public void visitReference(final ReferenceFacetMapping referenceMapping) {
            final Object columnValue = columnValues.next();
            hydrator.resolveElements(referenceMapping.getElementMapping(),
                    referenceMapping.getColumnType().columnElements(columnValue),
                    hydrator -> referenceMapping.setFacetValue(pojo, columnValue, id -> hydrator.getPojo(referenceMapping.getElementMapping(), id)));
        }

        @Override
        public void visitScalar(ScalarFacetMapping scalarMapping) {
            scalarMapping.setFacetValue(pojo, columnValues.next());
        }
    }
}
