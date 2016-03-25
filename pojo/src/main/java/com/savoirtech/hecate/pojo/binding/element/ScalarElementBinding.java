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

package com.savoirtech.hecate.pojo.binding.element;

import java.util.function.Predicate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.binding.ElementBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public class ScalarElementBinding implements ElementBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Converter converter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarElementBinding(Converter converter) {
        this.converter = converter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ElementBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getElementDataType() {
        return converter.getDataType();
    }

    @Override
    public Class<?> getElementType() {
        return converter.getValueType();
    }

    @Override
    public Object toColumnValue(Object facetElementValue) {
        return converter.toColumnValue(facetElementValue);
    }

    @Override
    public Object toFacetValue(Object columnValue, PojoQueryContext context) {
        return converter.toFacetValue(columnValue);
    }

    @Override
    public void visitChild(Object facetElementValue, Predicate<Facet> predicate, PojoVisitor visitor) {
        // Do nothing!
    }
}
