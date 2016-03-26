/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.binding.key.simple;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.binding.column.SimpleColumnBinding;
import com.savoirtech.hecate.pojo.binding.facet.SimpleFacetBinding;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class SimpleKeyBinding extends SimpleColumnBinding implements KeyBinding {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SimpleKeyBinding(Facet facet, String columnName, Converter converter) {
        super(facet, columnName, converter);
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void create(Create create) {
        create.addPartitionKey(getColumnName(), getConverter().getDataType());
    }

//----------------------------------------------------------------------------------------------------------------------
// KeyBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public ColumnBinding createReferenceBinding(Facet referenceFacet, PojoBinding<?> pojoBinding, NamingStrategy namingStrategy) {
        Facet facet = new SubFacet(referenceFacet, getFacet(), false);
        String tableName = namingStrategy.getReferenceTableName(referenceFacet);
        String columnName = namingStrategy.getColumnName(facet);
        return new SimpleReferenceBinding(referenceFacet, facet, columnName, getConverter(), pojoBinding, tableName);
    }

    @Override
    public void delete(Delete.Where delete) {
        delete.and(eq(getColumnName(), bindMarker()));
    }

    @Override
    public boolean isNullElement(Object element) {
        return element == null;
    }

    @Override
    public List<Object> elementToKeys(Object element) {
        return Collections.singletonList(element);
    }

    @Override
    public DataType getElementDataType() {
        return getConverter().getDataType();
    }

    @Override
    public Object getElementValue(Object pojo) {
        return getConverter().toColumnValue(getFacet().getValue(pojo));
    }

    @Override
    public List<Object> getKeyParameters(List<Object> keys) {
        return keys.stream().map(facetValue -> getConverter().toColumnValue(facetValue)).collect(Collectors.toList());
    }

    @Override
    public void selectWhere(Select.Where select) {
        select.and(eq(getColumnName(), bindMarker()));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void visitFacetChildren(Object facetValue, Predicate<Facet> predicate, PojoVisitor visitor) {
        // Do nothing (no children)!
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class SimpleReferenceBinding extends SimpleFacetBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final Facet referenceFacet;
        private final PojoBinding<?> pojoBinding;
        private final String tableName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public SimpleReferenceBinding(Facet referenceFacet, Facet facet, String columnName, Converter converter, PojoBinding<?> pojoBinding, String tableName) {
            super(facet, columnName, converter);
            this.referenceFacet = referenceFacet;
            this.pojoBinding = pojoBinding;
            this.tableName = tableName;
        }

//----------------------------------------------------------------------------------------------------------------------
// ColumnBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public void injectValues(Object pojo, Iterator<Object> columnValues, PojoQueryContext context) {
            Object key = columnValues.next();
            if (key == null) {
                referenceFacet.setValue(pojo, null);
            } else {
                referenceFacet.setValue(pojo, context.createPojo(pojoBinding, tableName, Collections.singletonList(key)));
            }
        }

        @Override
        public void visitChildren(Object pojo, Predicate<Facet> predicate, PojoVisitor visitor) {
            Object referenced = referenceFacet.getValue(pojo);
            if(referenced != null && predicate.test(referenceFacet)) {
                visitFacetChild(referenced, pojoBinding, predicate, visitor);
            }
        }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

        @SuppressWarnings("unchecked")
        private <P> void visitFacetChild(Object facetValue, PojoBinding<P> binding, Predicate<Facet> predicate, PojoVisitor visitor) {
            visitor.visit((P)facetValue, binding, tableName, predicate);
        }
    }
}
