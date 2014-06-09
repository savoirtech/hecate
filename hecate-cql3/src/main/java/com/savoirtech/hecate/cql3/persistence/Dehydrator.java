package com.savoirtech.hecate.cql3.persistence;

public interface Dehydrator {
    Object getIdentifier(Class<?> pojoType, Object pojo);
}
