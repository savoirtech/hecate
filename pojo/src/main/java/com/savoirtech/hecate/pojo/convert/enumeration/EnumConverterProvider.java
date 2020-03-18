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

package com.savoirtech.hecate.pojo.convert.enumeration;


import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterProvider;

public class EnumConverterProvider implements ConverterProvider {
//----------------------------------------------------------------------------------------------------------------------
// ConverterProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Class<? extends Converter> converterType() {
        return EnumConverter.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Converter createConverter(Class<?> valueType) {
        return new EnumConverter((Class<? extends Enum>) valueType);
    }

    @Override
    public Class<?> getValueType() {
        return Enum.class;
    }
}
