package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.value.Value;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;

public class FieldValue implements Value {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Field field;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FieldValue(Field field) {
        this.field = field;
    }

//----------------------------------------------------------------------------------------------------------------------
// Value Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Object get(Object pojo) {
        return ReflectionUtils.getFieldValue(field, pojo);
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return field.getAnnotation(annotationType);
    }

    @Override
    public Type getGenericType() {
        return field.getGenericType();
    }

    @Override
    public String getName() {
        return field.getName();
    }

    @Override
    public Class<?> getType() {
        return field.getType();
    }

    @Override
    public void set(Object pojo, Object value) {
        ReflectionUtils.setFieldValue(field, pojo, value);
    }
}
