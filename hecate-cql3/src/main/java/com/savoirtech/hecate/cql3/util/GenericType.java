package com.savoirtech.hecate.cql3.util;

import com.savoirtech.hecate.cql3.exception.HecateException;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public GenericType getListElementType() {
        return getTypeArgument(LIST_ELEMENT_TYPE_VAR, List.class);
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

    public <T> GenericType getTypeArgument(TypeVariable<Class<T>> variable, Class<T> toType) {
        final Map<TypeVariable<?>, Type> typeArguments = TypeUtils.getTypeArguments(type, toType);
        if (typeArguments != null) {
            final Type arg = typeArguments.get(variable);
            if (arg != null) {
                return new GenericType(declaringClass, arg);
            }
        }
        throw new HecateException(String.format("Unable to resolve type argument %s.", variable));
    }
}
