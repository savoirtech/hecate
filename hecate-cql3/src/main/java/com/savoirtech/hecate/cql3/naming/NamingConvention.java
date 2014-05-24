package com.savoirtech.hecate.cql3.naming;

import java.lang.reflect.Field;

public interface NamingConvention {
    String tableName(Class<?> pojoType);
    String tableName(Field field);
    String columnName(Field field);
    boolean isIdentifier(Field field);
}
