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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.Table;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.binding.column.SimpleColumnBinding;
import com.savoirtech.hecate.pojo.binding.facet.SimpleFacetBinding;
import com.savoirtech.hecate.pojo.binding.key.component.KeyComponent;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

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
    public void describe(Table table, Schema schema) {
        table.addPartitionKey(getColumnName(), getConverter().getDataType());
    }

    @Override
    public void verifySchema(KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata) {
        verifyPartitionKeyColumn(tableMetadata, getColumnName(), getDataType());
    }

//----------------------------------------------------------------------------------------------------------------------
// KeyBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public KeyComponent createClusteringColumnReferenceComponent(Facet referenceFacet, PojoBinding<?> pojoBinding, NamingStrategy namingStrategy) {
        Facet facet = new SubFacet(referenceFacet, getFacet(), false);
        String tableName = namingStrategy.getReferenceTableName(referenceFacet);
        String columnName = namingStrategy.getColumnName(facet);
        return new SimpleClusteringColumnReferenceComponent(referenceFacet, facet, columnName, getConverter(), pojoBinding, tableName);
    }

    @Override
    public ColumnBinding createReferenceBinding(Facet referenceFacet, PojoBinding<?> pojoBinding, NamingStrategy namingStrategy) {
        Facet facet = new SubFacet(referenceFacet, getFacet(), false);
        String tableName = namingStrategy.getReferenceTableName(referenceFacet);
        String columnName = namingStrategy.getColumnName(facet);
        return new SimpleReferenceBinding(referenceFacet, facet, columnName, getConverter(), pojoBinding, tableName);
    }

    @Override
    public Delete delete(OngoingWhereClause<Delete> delete) {
        return delete.whereColumn(getColumnName()).isEqualTo(bindMarker());
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
    public Select selectWhere(Select select) {
        return select.whereColumn(getColumnName()).isEqualTo(bindMarker());
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

    private static class SimpleClusteringColumnReferenceComponent extends SimpleReferenceBinding implements KeyComponent {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public SimpleClusteringColumnReferenceComponent(Facet referenceFacet, Facet facet, String columnName, Converter converter, PojoBinding<?> pojoBinding, String tableName) {
            super(referenceFacet, facet, columnName, converter, pojoBinding, tableName);
        }

//----------------------------------------------------------------------------------------------------------------------
// ColumnBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public void describe(Table table, Schema schema) {
            table.addClusteringColumn(getColumnName(), getConverter().getDataType());
            getPojoBinding().describe(schema.createTable(getTableName()), schema);
        }

//----------------------------------------------------------------------------------------------------------------------
// KeyComponent Implementation
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public ColumnBinding createReferenceBinding(Facet referencingFacet, NamingStrategy strategy) {
            Facet facet = new SubFacet(referencingFacet, getFacet(), false);
            String tableName = strategy.getReferenceTableName(getReferenceFacet());
            String columnName = strategy.getColumnName(facet);
            return new SimpleReferenceBinding(referencingFacet, facet, columnName, getConverter(), getPojoBinding(), tableName);
        }

        @Override
        public Delete delete(OngoingWhereClause<Delete> delete) {
            return delete.whereColumn(getColumnName()).isEqualTo(bindMarker());
        }

        @Override
        public int getOrder() {
            return getFacet().getAnnotation(PartitionKey.class).order();
        }

        @Override
        public int getRank() {
            return 2;
        }
    }

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
        public void describe(Table table, Schema schema) {
            super.describe(table, schema);
            pojoBinding.describe(schema.createTable(tableName), schema);
        }

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
        public void verifySchema(KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata) {
            super.verifySchema(keyspaceMetadata, tableMetadata);
            pojoBinding.verifySchema(keyspaceMetadata, tableName);
        }

        @Override
        public void visitChildren(Object pojo, Predicate<Facet> predicate, PojoVisitor visitor) {
            Object referenced = referenceFacet.getValue(pojo);
            if (referenced != null && predicate.test(referenceFacet)) {
                visitFacetChild(referenced, pojoBinding, predicate, visitor);
            }
        }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

        public PojoBinding<?> getPojoBinding() {
            return pojoBinding;
        }

        public Facet getReferenceFacet() {
            return referenceFacet;
        }

        public String getTableName() {
            return tableName;
        }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

        @SuppressWarnings("unchecked")
        private <P> void visitFacetChild(Object facetValue, PojoBinding<P> binding, Predicate<Facet> predicate, PojoVisitor visitor) {
            visitor.visit((P) facetValue, binding, tableName, predicate);
        }
    }
}
