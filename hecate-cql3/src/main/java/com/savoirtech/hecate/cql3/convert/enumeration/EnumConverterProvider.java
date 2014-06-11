/*
 * Copyright (c) 2014. Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.convert.enumeration;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterProvider;

public class EnumConverterProvider implements ValueConverterProvider {
//----------------------------------------------------------------------------------------------------------------------
// ValueConverterProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Class<? extends ValueConverter> converterType() {
        return EnumConverter.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ValueConverter createConverter(Class<?> valueType) {
        return new EnumConverter((Class<? extends Enum>) valueType);
    }
}
