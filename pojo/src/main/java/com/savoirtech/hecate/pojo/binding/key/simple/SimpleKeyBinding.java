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
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.binding.column.SimpleColumnBinding;
import com.savoirtech.hecate.pojo.binding.key.component.KeyComponent;
import com.savoirtech.hecate.pojo.binding.key.component.PartitionKeyComponent;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;

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
    public void delete(Delete.Where delete) {
        delete.and(eq(getColumnName(), bindMarker()));
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
    public Class<?> getElementType() {
        return getConverter().getValueType();
    }

    @Override
    public Object getElementValue(Object pojo) {
        return getConverter().toColumnValue(getFacet().getValue(pojo));
    }

    @Override
    public List<KeyComponent> getKeyComponents() {
        return Collections.singletonList(new PartitionKeyComponent(getFacet(), getColumnName(), getConverter()));
    }

    @Override
    public List<Object> getKeyParameters(Object pojo) {
        return Collections.singletonList(getConverter().toColumnValue(getFacetValue(pojo)));
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
    protected void visitFacetChildren(Object facetValue, PojoVisitor visitor) {
        // Do nothing (no children)!
    }
}
