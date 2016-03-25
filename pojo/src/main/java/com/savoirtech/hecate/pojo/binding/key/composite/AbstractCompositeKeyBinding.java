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

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
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

public class AbstractCompositeKeyBinding extends NestedColumnBinding<KeyComponent> implements KeyBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final TupleType elementType;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    protected static KeyComponent createComponent(Facet facet, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        Converter converter = converterRegistry.getConverter(facet.getType());
        if (converter == null) {
            throw new HecateException("No converter found for composite key facet \"%s\" (%s)", facet.getName(), facet.getType().getRawType().getCanonicalName());
        }
        if (facet.hasAnnotation(PartitionKey.class)) {
            return new PartitionKeyComponent(facet, namingStrategy.getColumnName(facet), converter);
        } else if (facet.hasAnnotation(ClusteringColumn.class)) {
            return new ClusteringColumnComponent(facet, namingStrategy.getColumnName(facet), converter);
        } else {
            throw new HecateException("Composite key facets mixed with composite key object facets.");
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractCompositeKeyBinding(List<KeyComponent> components) {
        super(sorted(components));
        List<DataType> dataTypes = components.stream().map(KeyComponent::getDataType).collect(Collectors.toList());
        elementType = TupleType.of(dataTypes.toArray(new DataType[dataTypes.size()]));
    }

    private static List<KeyComponent> sorted(List<KeyComponent> components) {
        Collections.sort(components, (left, right) -> new CompareToBuilder().append(left.getRank(), right.getRank()).append(left.getOrder(), right.getOrder()).build());
        return components;
    }

//----------------------------------------------------------------------------------------------------------------------
// KeyBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

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
    public Class<?> getElementType() {
        return TupleValue.class;
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
        private final Facet referenceFacet;
        private final PojoBinding<?> pojoBinding;
        private final String tableName;

        public CompositeKeyReferenceBinding(Facet referenceFacet, PojoBinding<?> pojoBinding, String tableName, List<ColumnBinding> componentBindings) {
            super(componentBindings);
            this.referenceFacet = referenceFacet;
            this.pojoBinding = pojoBinding;
            this.tableName = tableName;
        }

        private boolean isNull(List<Object> keys) {
            Optional<Object> nonNull = keys.stream().filter(key -> key != null).findFirst();
            return !nonNull.isPresent();
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
        public void visitChildren(Object pojo, Predicate<Facet> predicate, PojoVisitor visitor) {
            Object referenced = referenceFacet.getValue(pojo);
            if(referenced != null && predicate.test(referenceFacet)) {
                visitChild(referenced, pojoBinding, tableName, predicate, visitor);
            }
        }

        @SuppressWarnings("unchecked")
        private <P> void visitChild(Object pojo, PojoBinding<P> binding, String tableName, Predicate<Facet> predicate, PojoVisitor visitor) {
            visitor.visit((P)pojo, binding, tableName, predicate);
        }
    }
}
