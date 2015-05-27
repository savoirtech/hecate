/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.joda.time;

import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterProvider;

public abstract class JodaTimeConverterProvider implements ConverterProvider {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> baseType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    protected JodaTimeConverterProvider(Class<?> baseType) {
        this.baseType = baseType;
    }

//----------------------------------------------------------------------------------------------------------------------
// ConverterProvider Implementation
//----------------------------------------------------------------------------------------------------------------------
    
    @Override
    public Class<? extends Converter> converterType() {
        return JodaTimeConverter.class;
    }

    @Override
    public Converter createConverter(Class<?> jodaType) {
        return new JodaTimeConverter(jodaType);
    }

    @Override
    public Class<?> getValueType() {
        return baseType;
    }
}
