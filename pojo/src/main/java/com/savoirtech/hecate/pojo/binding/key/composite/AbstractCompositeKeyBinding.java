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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.core.util.CqlUtils;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.ParameterBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.binding.column.AbstractColumnBinding;
import com.savoirtech.hecate.pojo.binding.key.component.ClusteringColumnComponent;
import com.savoirtech.hecate.pojo.binding.key.component.KeyComponent;
import com.savoirtech.hecate.pojo.binding.key.component.PartitionKeyComponent;
import com.savoirtech.hecate.pojo.binding.key.component.SimpleKeyComponent;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;
import org.apache.commons.lang3.builder.CompareToBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class AbstractCompositeKeyBinding extends AbstractColumnBinding implements KeyBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final List<KeyComponent> components;
    private final TupleType elementType;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    protected static SimpleKeyComponent createComponent(Facet facet, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        Converter converter = converterRegistry.getConverter(facet.getType());
        if(converter == null) {
            throw new HecateException("No converter found for composite key facet \"%s\" (%s)", facet.getName(), facet.getType().getRawType().getCanonicalName());
        }
        if(facet.hasAnnotation(PartitionKey.class)) {
            return new PartitionKeyComponent(facet, namingStrategy.getColumnName(facet), converter);
        } else if(facet.hasAnnotation(ClusteringColumn.class)) {
            return new ClusteringColumnComponent(facet, namingStrategy.getColumnName(facet), converter);
        } else {
            throw new HecateException("Composite key facets mixed with composite key object facets.");
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractCompositeKeyBinding(List<KeyComponent> components) {
        this.components = components;
        Collections.sort(components, (left, right) -> new CompareToBuilder().append(left.getRank(), right.getRank()).append(left.getOrder(), right.getOrder()).build());
        List<DataType> dataTypes = components.stream().map(KeyComponent::getDataType).collect(Collectors.toList());
        elementType = TupleType.of(dataTypes.toArray(new DataType[dataTypes.size()]));
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnBinding Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public void collectParameters(Object pojo, List<Object> parameters) {
        forEachComponent(c -> c.collectParameters(pojo, parameters));
    }

    @Override
    public void create(Create create) {
        forEachComponent(c -> c.create(create));
    }

    @Override
    public List<ParameterBinding> getParameterBindings() {
        return components.stream().flatMap(c -> c.getParameterBindings().stream()).collect(Collectors.toList());
    }

    @Override
    public void injectValues(Object pojo, Iterator<Object> columnValues, PojoQueryContext context) {
        forEachComponent(c -> c.injectValues(pojo, columnValues, context));
    }

    @Override
    public void insert(Insert insert) {
        forEachComponent(col -> col.insert(insert));
    }

    @Override
    public void select(Select.Selection select) {
        forEachComponent(col -> col.select(select));
    }

    @Override
    public void verifySchema(TableMetadata metadata) {
        forEachComponent(c -> c.verifySchema(metadata));
    }

    @Override
    public void visitChildren(Object pojo, Predicate<Facet> predicate, PojoVisitor visitor) {
        forEachComponent(c -> c.visitChildren(pojo, predicate, visitor));
    }

//----------------------------------------------------------------------------------------------------------------------
// KeyBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void delete(Delete.Where delete) {
        forEachComponent(c -> c.delete(delete));
    }

    @Override
    public List<Object> elementToKeys(Object element) {
        return CqlUtils.toList((TupleValue)element);
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
        forEachComponent(c -> c.collectParameters(pojo, parameters));
        return elementType.newValue(parameters.toArray(new Object[parameters.size()]));
    }

    @Override
    public List<KeyComponent> getKeyComponents() {
        return components;
    }

    @Override
    public List<Object> getKeyParameters(Object pojo) {
        List<Object> parameters = new LinkedList<>();
        forEachComponent(c -> c.collectParameters(pojo, parameters));
        return parameters;
    }

    @Override
    public List<Object> getKeyParameters(List<Object> keys) {
        Iterator<Object> iterator = keys.iterator();
        return components.stream().map(c -> c.toColumnValue(iterator.next())).collect(Collectors.toList());
    }

    @Override
    public void selectWhere(Select.Where select) {
        components.stream().forEach(c -> select.and(eq(c.getColumnName(), bindMarker())));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private void forEachComponent(Consumer<KeyComponent> consumer) {
        components.stream().forEach(consumer);
    }
}
