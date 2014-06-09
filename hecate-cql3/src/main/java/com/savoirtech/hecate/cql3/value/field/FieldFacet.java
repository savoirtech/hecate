package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.util.GenericType;
import com.savoirtech.hecate.cql3.value.Facet;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

public class FieldFacet implements Facet {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Field field;
    private final GenericType type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FieldFacet(Class<?> pojoType, Field field) {
        this.field = field;
        this.type = new GenericType(pojoType, field.getGenericType());
    }

//----------------------------------------------------------------------------------------------------------------------
// Value Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Object get(Object pojo) {
        return ReflectionUtils.getFieldValue(field, pojo);
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return field.getAnnotation(annotationType);
    }

    @Override
    public String getName() {
        return field.getName();
    }

    public GenericType getType() {
        return type;
    }

    @Override
    public void set(Object pojo, Object value) {
        ReflectionUtils.setFieldValue(field, pojo, value);
    }
}
