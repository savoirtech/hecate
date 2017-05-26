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

package com.savoirtech.hecate.pojo.binding.key.composite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.core.schema.Table;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.binding.column.NestedColumnBinding;
import com.savoirtech.hecate.pojo.binding.key.component.ClusteringColumnComponent;
import com.savoirtech.hecate.pojo.binding.key.component.KeyComponent;
import com.savoirtech.hecate.pojo.binding.key.component.PartitionKeyComponent;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import org.apache.commons.lang3.builder.CompareToBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class CompositeKeyBinding extends NestedColumnBinding<KeyComponent> implements KeyBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final TupleType elementType;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    protected static KeyComponent createComponent(Facet facet, ConverterRegistry converterRegistry, NamingStrategy namingStrategy, PojoBindingFactory pojoBindingFactory) {
        Converter converter = converterRegistry.getConverter(facet.getType());
        if (facet.hasAnnotation(PartitionKey.class)) {
            if (converter == null) {
                throw new HecateException("No converter found for @PartitionKey facet \"%s\".", facet.getName());
            }
            return new PartitionKeyComponent(facet, namingStrategy.getColumnName(facet), converter);
        } else {
            if (converter == null) {
                PojoBinding<?> pojoBinding = pojoBindingFactory.createPojoBinding(facet.getType().getRawType());
                return pojoBinding.getKeyBinding().createClusteringColumnReferenceComponent(facet, pojoBinding, namingStrategy);
            } else
                return new ClusteringColumnComponent(facet, namingStrategy.getColumnName(facet), converter);
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public CompositeKeyBinding(List<Facet> keyFacets, NamingStrategy namingStrategy, ConverterRegistry converterRegistry, PojoBindingFactory pojoBindingFactory) {
        keyFacets.stream()
                .map(facet -> createComponent(facet, converterRegistry, namingStrategy, pojoBindingFactory))
                .sorted((left, right) -> new CompareToBuilder().append(left.getRank(), right.getRank()).append(left.getOrder(), right.getOrder()).build())
                .forEach(this::addBinding);
        List<DataType> dataTypes = mapBindings(KeyComponent::getDataType).collect(Collectors.toList());
        elementType = TupleType.of(dataTypes.toArray(new DataType[dataTypes.size()]));
    }

//----------------------------------------------------------------------------------------------------------------------
// KeyBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public KeyComponent createClusteringColumnReferenceComponent(Facet parent, PojoBinding<?> pojoBinding, NamingStrategy namingStrategy) {
        throw new HecateException("@ClusteringColumn facet \"%s\" references class with composite keys", parent.getName());
    }

    @Override
    public ColumnBinding createReferenceBinding(Facet referenceFacet, PojoBinding<?> pojoBinding, NamingStrategy namingStrategy) {
        final String tableName = namingStrategy.getReferenceTableName(referenceFacet);
        List<ColumnBinding> componentBindings = mapBindings(binding -> binding.createReferenceBinding(referenceFacet, namingStrategy)).collect(Collectors.toList());
        return new CompositeKeyReferenceBinding(referenceFacet, pojoBinding, tableName, componentBindings);
    }

    @Override
    public void delete(Delete.Where delete) {
        forEachBinding(c -> c.delete(delete));
    }

    @Override
    public List<Object> elementToKeys(Object element) {
        return CqlUtils.toList((TupleValue) element);
    }

    @Override
    public DataType getElementDataType() {
        return elementType;
    }

    @Override
    public Object getElementValue(Object pojo) {
        List<Object> parameters = new LinkedList<>();
        forEachBinding(c -> c.collectParameters(pojo, parameters));
        return elementType.newValue(parameters.toArray(new Object[parameters.size()]));
    }

    @Override
    public List<Object> getKeyParameters(List<Object> keys) {
        Iterator<Object> iterator = keys.iterator();
        return mapBindings(c -> c.toColumnValue(iterator.next())).collect(Collectors.toList());
    }

    @Override
    public void selectWhere(Select.Where select) {
        forEachBinding(c -> select.and(eq(c.getColumnName(), bindMarker())));
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class CompositeKeyReferenceBinding extends NestedColumnBinding<ColumnBinding> implements ColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final Facet referenceFacet;
        private final PojoBinding<?> pojoBinding;
        private final String tableName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public CompositeKeyReferenceBinding(Facet referenceFacet, PojoBinding<?> pojoBinding, String tableName, List<ColumnBinding> componentBindings) {
            super(componentBindings);
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
            List<Object> keys = new ArrayList<>(getBindings().size());
            forEachBinding(binding -> keys.add(columnValues.next()));
            if (isNull(keys)) {
                referenceFacet.setValue(pojo, null);
            } else {
                referenceFacet.setValue(pojo, context.createPojo(pojoBinding, tableName, keys));
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
                visitChild(referenced, pojoBinding, tableName, predicate, visitor);
            }
        }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

        private boolean isNull(List<Object> keys) {
            Optional<Object> nonNull = keys.stream().filter(Objects::nonNull).findFirst();
            return !nonNull.isPresent();
        }

        @SuppressWarnings("unchecked")
        private <P> void visitChild(Object pojo, PojoBinding<P> binding, String tableName, Predicate<Facet> predicate, PojoVisitor visitor) {
            visitor.visit((P) pojo, binding, tableName, predicate);
        }
    }
}
