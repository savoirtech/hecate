package com.savoirtech.hecate.cql3.value;

import com.google.common.reflect.TypeParameter;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Map;

public class ValueType {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> declaringClass;
    private final Class<?> rawType;
    private final Type type;

    public ValueType(Class<?> declaringClass, Type type) {
        this.declaringClass = declaringClass;
        this.type = type;
        this.rawType = TypeUtils.getRawType(type, declaringClass);
    }

    public Class<?> getRawType() {
        return rawType;
    }

    public <T> Type getTypeArgument(TypeParameter<Class<T>> variable, Class<T> declaringType) {
        final Map<TypeVariable<?>, Type> arguments = TypeUtils.getTypeArguments(type, declaringType);

        return arguments.get(variable);
    }


}
