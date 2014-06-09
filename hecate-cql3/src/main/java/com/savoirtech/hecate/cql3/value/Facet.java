package com.savoirtech.hecate.cql3.value;

import com.savoirtech.hecate.cql3.util.GenericType;

import java.lang.annotation.Annotation;

public interface Facet {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Object get(Object pojo);

    <A extends Annotation> A getAnnotation(Class<A> annotationType);

    GenericType getType();

    String getName();

    void set(Object pojo, Object value);
}
