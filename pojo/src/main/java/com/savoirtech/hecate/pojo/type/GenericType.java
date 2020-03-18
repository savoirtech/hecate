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

package com.savoirtech.hecate.pojo.type;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.savoirtech.hecate.core.exception.HecateException;
import org.apache.commons.lang3.reflect.TypeUtils;

public class GenericType {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final TypeVariable<Class<List>> LIST_ELEMENT_TYPE_VAR = List.class.getTypeParameters()[0];
    private static final TypeVariable<Class<Set>> SET_ELEMENT_TYPE_VAR = Set.class.getTypeParameters()[0];
    private static final TypeVariable<Class<Map>> MAP_KEY_TYPE_VAR = Map.class.getTypeParameters()[0];
    private static final TypeVariable<Class<Map>> MAP_VALUE_TYPE_VAR = Map.class.getTypeParameters()[1];

    private final Class<?> declaringClass;
    private final Class<?> rawType;
    private final Type type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public GenericType(Class<?> declaringClass, Type type) {
        this.declaringClass = declaringClass;
        this.type = type;
        this.rawType = TypeUtils.getRawType(type, declaringClass);
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Class<?> getRawType() {
        return rawType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        return String.valueOf(type);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public GenericType getArrayElementType() {
        return new GenericType(declaringClass, rawType.getComponentType());
    }

    public GenericType getElementType() {
        if (isSet()) {
            return getSetElementType();
        } else if (isMap()) {
            return getMapValueType();
        } else if (isList()) {
            return getListElementType();
        } else if (isArray()) {
            return getArrayElementType();
        }
        return this;
    }

    public GenericType getListElementType() {
        return getTypeArgument(LIST_ELEMENT_TYPE_VAR, List.class);
    }

    private <T> GenericType getTypeArgument(TypeVariable<Class<T>> variable, Class<T> toType) {
        final Map<TypeVariable<?>, Type> typeArguments = TypeUtils.getTypeArguments(type, toType);
        if (typeArguments != null) {
            final Type arg = typeArguments.get(variable);
            if (arg != null) {
                return new GenericType(declaringClass, arg);
            }
        }
        throw new HecateException("Unable to resolve type argument %s.", variable);
    }

    public GenericType getMapKeyType() {
        return getTypeArgument(MAP_KEY_TYPE_VAR, Map.class);
    }

    public GenericType getMapValueType() {
        return getTypeArgument(MAP_VALUE_TYPE_VAR, Map.class);
    }

    public GenericType getSetElementType() {
        return getTypeArgument(SET_ELEMENT_TYPE_VAR, Set.class);
    }

    public boolean isArray() {
        return rawType.isArray();
    }

    public boolean isList() {
        return List.class.equals(rawType);
    }

    public boolean isMap() {
        return Map.class.equals(rawType);
    }

    public boolean isSet() {
        return Set.class.equals(rawType);
    }
}
