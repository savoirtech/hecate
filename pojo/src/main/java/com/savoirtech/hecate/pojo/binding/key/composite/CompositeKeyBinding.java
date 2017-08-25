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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.binding.column.NestedColumnBinding;
import com.savoirtech.hecate.pojo.binding.key.component.ClusteringColumnComponent;
import com.savoirtech.hecate.pojo.binding.key.component.KeyComponent;
import com.savoirtech.hecate.pojo.binding.key.component.PartitionKeyComponent;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import org.apache.commons.lang3.builder.CompareToBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class CompositeKeyBinding extends NestedColumnBinding<KeyComponent> implements KeyBinding {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static KeyComponent createComponent(Facet facet, ConverterRegistry converterRegistry, NamingStrategy namingStrategy, PojoBindingFactory pojoBindingFactory) {
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
        throw new HecateException("Composite keys cannot be used as foreign keys.");
    }

    @Override
    public void delete(Delete.Where delete) {
        forEachBinding(c -> c.delete(delete));
    }

    @Override
    public List<Object> elementToKeys(Object element) {
        throw new HecateException("Composite keys cannot be used as foreign keys.");
    }

    @Override
    public DataType getElementDataType() {
        throw new HecateException("Composite keys cannot be used as foreign keys.");
    }

    @Override
    public Object getElementValue(Object pojo) {
        throw new HecateException("Composite keys cannot be used as foreign keys.");
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
}
