package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.value.Value;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

public class PropertyValue implements Value {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private PropertyDescriptor propertyDescriptor;
    private final Method readMethod;
    private final Method writeMethod;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PropertyValue(PropertyDescriptor propertyDescriptor, Method readMethod, Method writeMethod) {
        this.propertyDescriptor = propertyDescriptor;
        this.readMethod = readMethod;
        this.writeMethod = writeMethod;
    }

//----------------------------------------------------------------------------------------------------------------------
// Value Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object get(Object pojo) {
        return ReflectionUtils.invoke(pojo, readMethod);
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return readMethod.getAnnotation(annotationType);
    }

    @Override
    public Type getGenericType() {
        return readMethod.getGenericReturnType();
    }

    @Override
    public String getName() {
        return propertyDescriptor.getName();
    }

    @Override
    public Class<?> getType() {
        return readMethod.getReturnType();
    }

    @Override
    public void set(Object pojo, Object value) {
        ReflectionUtils.invoke(pojo, writeMethod, value);
    }
}
