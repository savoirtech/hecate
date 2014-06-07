package com.savoirtech.hecate.cql3.value;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

public interface Value {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Object get(Object pojo);

    <A extends Annotation> A getAnnotation(Class<A> annotationType);

    Type getGenericType();

    String getName();

    Class<?> getType();

    void set(Object pojo, Object value);
}
