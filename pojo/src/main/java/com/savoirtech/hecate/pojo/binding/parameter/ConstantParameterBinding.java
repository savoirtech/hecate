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

package com.savoirtech.hecate.pojo.binding.parameter;

import com.savoirtech.hecate.pojo.facet.Facet;

public class ConstantParameterBinding extends AbstractParameterBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Object value;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ConstantParameterBinding(Facet facet, String columnName, Object value) {
        super(facet, columnName);
        this.value = value;
    }

//----------------------------------------------------------------------------------------------------------------------
// ParameterBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object toColumnValue(Object facetValue) {
        return value;
    }
}