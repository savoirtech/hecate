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

package com.savoirtech.hecate.pojo.mapping.element;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.Hydrator;

import java.util.Collections;

public class PojoElementHandler implements ElementHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoMapping<Object> pojoMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public PojoElementHandler(PojoMapping<?> pojoMapping) {
        this.pojoMapping = (PojoMapping<Object>)pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// ElementHandler Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return pojoMapping.getForeignKeyMapping().getColumnType().getDataType();
    }

    @Override
    public Object getInsertValue(Object pojo, Dehydrator dehydrator) {
        dehydrator.dehydrate(pojoMapping, Collections.singleton(pojo));
        return pojoMapping.getForeignKeyMapping().getFacet().getValue(pojo);
    }

    @Override
    public Object getParameterValue(Object pojo) {
        return pojoMapping.getForeignKeyMapping().getFacet().getValue(pojo);
    }

    @Override
    public boolean isCascadable() {
        return true;
    }

    @Override
    public void resolveElements(Iterable<Object> cassandraValue, Hydrator hydrator,ElementInjector injector) {
        hydrator.resolveElements(pojoMapping,cassandraValue,injector);
    }
}
