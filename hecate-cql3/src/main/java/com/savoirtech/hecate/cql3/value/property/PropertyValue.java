package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.util.GenericType;
import com.savoirtech.hecate.cql3.value.Value;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class PropertyValue implements Value {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private PropertyDescriptor propertyDescriptor;
    private final Method readMethod;
    private final Method writeMethod;
    private final GenericType type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PropertyValue(Class<?> declaringClass, PropertyDescriptor propertyDescriptor, Method readMethod, Method writeMethod) {
        this.propertyDescriptor = propertyDescriptor;
        this.readMethod = readMethod;
        this.writeMethod = writeMethod;
        this.type = new GenericType(declaringClass, readMethod.getGenericReturnType());
    }

//----------------------------------------------------------------------------------------------------------------------
// Value Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Object get(Object pojo) {
        return ReflectionUtils.invoke(pojo, readMethod);
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return readMethod.getAnnotation(annotationType);
    }

    @Override
    public String getName() {
        return propertyDescriptor.getName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public GenericType getType() {
        return type;
    }

    @Override
    public void set(Object pojo, Object value) {
        ReflectionUtils.invoke(pojo, writeMethod, value);
    }
}
