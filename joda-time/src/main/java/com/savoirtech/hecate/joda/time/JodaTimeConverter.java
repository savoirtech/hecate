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


import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.convert.Converter;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class JodaTimeConverter implements Converter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> valueType;
    private final Method parseMethod;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public JodaTimeConverter(Class<?> valueType) {
        this.valueType = valueType;
        if (Modifier.isAbstract(valueType.getModifiers())) {
            throw new HecateException("Cannot create converter for non-concrete type %s.", valueType.getCanonicalName());
        }
        this.parseMethod = findParseMethod(valueType);
    }

    private static Method findParseMethod(Class<?> jodaType) {
        Method m = MethodUtils.getMatchingAccessibleMethod(jodaType, "parse", String.class);
        if (m == null) {
            m = MethodUtils.getMatchingAccessibleMethod(jodaType, "parse" + jodaType.getSimpleName(), String.class);
        }
        if (m == null) {
            throw new HecateException("No parse() or parse%s() method found on class %s.", jodaType.getSimpleName(), jodaType.getCanonicalName());
        }
        return m;
    }

//----------------------------------------------------------------------------------------------------------------------
// Converter Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataTypes.TEXT;
    }

    @Override
    public Class<?> getValueType() {
        return valueType;
    }

    @Override
    public Object toColumnValue(Object value) {
        return value == null ? null : value.toString();
    }

    @Override
    public Object toFacetValue(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return parseMethod.invoke(null, value.toString());
        } catch (ReflectiveOperationException e) {
            throw new HecateException(e, "Unable to parse '%s' value into %s.", value.toString(), valueType.getCanonicalName());
        }
    }
}
