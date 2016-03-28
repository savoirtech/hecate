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
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public class PojoElementBinding implements ElementBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoBinding<?> pojoBinding;
    private final String tableName;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <T> void visitChild(Object child, PojoBinding<T> binding, String tableName, Predicate<Facet> predicate, PojoVisitor visitor) {
        visitor.visit((T) child, binding, tableName, predicate);
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoElementBinding(PojoBinding<?> pojoBinding, String tableName) {
        this.tableName = tableName;
        this.pojoBinding = pojoBinding;
    }

//----------------------------------------------------------------------------------------------------------------------
// ElementBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getElementDataType() {
        return pojoBinding.getKeyBinding().getElementDataType();
    }

    @Override
    public Class<?> getElementType() {
        return pojoBinding.getPojoType();
    }

    @Override
    public Object toColumnValue(Object facetElementValue) {
        return pojoBinding.getKeyBinding().getElementValue(facetElementValue);
    }

    @Override
    public Object toFacetValue(Object columnValue, PojoQueryContext context) {
        return context.createPojo(pojoBinding, tableName, pojoBinding.getKeyBinding().elementToKeys(columnValue));
    }

    @Override
    public void visitChild(Object facetElementValue, Predicate<Facet> predicate, PojoVisitor visitor) {
        if (facetElementValue != null) {
            visitChild(facetElementValue, pojoBinding, tableName, predicate, visitor);
        }
    }
}
