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

package com.savoirtech.hecate.cql3.handler.delegate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.util.Callback;
import com.savoirtech.hecate.cql3.value.Facet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PojoDelegate implements ColumnHandlerDelegate {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final FacetMetadata facetMetadata;
    private final PojoMetadata pojoMetadata;
    private final ValueConverter identifierConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDelegate(PojoMetadata pojoMetadata, FacetMetadata facetMetadata, ValueConverter identifierConverter) {
        this.pojoMetadata = pojoMetadata;
        this.facetMetadata = facetMetadata;
        this.identifierConverter = identifierConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandlerDelegate Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void collectDeletionIdentifiers(Iterable<Object> columnValues, DeleteContext context) {
        final Set<Object> identifiers = new HashSet<>();
        for (Object columnValue : columnValues) {
            identifiers.add(identifierConverter.fromCassandraValue(columnValue));
        }
        context.addDeletedIdentifiers(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifiers);
    }

    @Override
    public Object convertElement(Object parameterValue) {
        return identifierConverter.toCassandraValue(parameterValue);
    }

    @Override
    public Object convertToInsertValue(Object facetValue, SaveContext saveContext) {
        final Object identifier = identifierConverter.toCassandraValue(pojoMetadata.getIdentifierFacet().getFacet().get(facetValue));
        saveContext.addPojo(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifier, facetValue);
        return identifier;
    }

    @Override
    public void createValueConverter(Callback<ValueConverter> target, Iterable<Object> columnValues, QueryContext context) {
        final Set<Object> identifiers = new HashSet<>();
        for (Object columnValue : columnValues) {
            identifiers.add(identifierConverter.fromCassandraValue(columnValue));
        }
        context.addPojos(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifiers, new PojoResultsCallback(target, identifierConverter, pojoMetadata.getIdentifierFacet().getFacet()));
    }

    @Override
    public DataType getDataType() {
        return identifierConverter.getDataType();
    }

    @Override
    public boolean isCascading() {
        return true;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static final class PojoResultsCallback implements Callback<List<Object>> {
        private final Callback<ValueConverter> originalTarget;
        private final ValueConverter identifierConverter;
        private final Facet identifierFacet;

        private PojoResultsCallback(Callback<ValueConverter> originalTarget, ValueConverter identifierConverter, Facet identifierFacet) {
            this.originalTarget = originalTarget;
            this.identifierConverter = identifierConverter;
            this.identifierFacet = identifierFacet;
        }

        @Override
        public void execute(List<Object> pojos) {
            Map<Object, Object> pojoMap = new HashMap<>();
            for (Object pojo : pojos) {
                pojoMap.put(identifierFacet.get(pojo), pojo);
            }
            originalTarget.execute(new PojoValueConverter(identifierConverter, pojoMap));
        }
    }

    private static final class PojoValueConverter implements ValueConverter {
        private final ValueConverter identifierConverter;
        private final Map<Object, Object> pojoMap;

        private PojoValueConverter(ValueConverter identifierConverter, Map<Object, Object> pojoMap) {
            this.identifierConverter = identifierConverter;
            this.pojoMap = pojoMap;
        }

        @Override
        public Object fromCassandraValue(Object value) {
            Object identifier = identifierConverter.fromCassandraValue(value);
            return pojoMap.get(identifier);
        }

        @Override
        public DataType getDataType() {
            return identifierConverter.getDataType();
        }

        @Override
        public Object toCassandraValue(Object value) {
            return identifierConverter.toCassandraValue(value);
        }
    }
}
