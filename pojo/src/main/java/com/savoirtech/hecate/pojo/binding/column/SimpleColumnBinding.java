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

package com.savoirtech.hecate.pojo.binding.column;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.binding.ParameterBinding;
import com.savoirtech.hecate.pojo.binding.PojoVisitor;
import com.savoirtech.hecate.pojo.binding.parameter.SimpleParameterBinding;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.query.PojoQueryContext;

public abstract class SimpleColumnBinding extends SingleColumnBinding<Object,Object> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Converter converter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SimpleColumnBinding(Facet facet, String columnName, Converter converter) {
        super(facet, columnName);
        this.converter = converter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<ParameterBinding> getParameterBindings() {
        return Collections.singletonList(new SimpleParameterBinding(getFacet(), getColumnName(), converter));
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Converter getConverter() {
        return converter;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return converter.getDataType();
    }

    @Override
    public Object toColumnValue(Object facetValue) {
        return converter.toColumnValue(facetValue);
    }

    @Override
    public Object toFacetValue(Object columnValue, PojoQueryContext context) {
        return converter.toFacetValue(columnValue);
    }

    @Override
    protected void visitFacetChildren(Object facetValue, Predicate<Facet> predicate, PojoVisitor visitor) {
        // Do nothing (no children)!
    }
}
