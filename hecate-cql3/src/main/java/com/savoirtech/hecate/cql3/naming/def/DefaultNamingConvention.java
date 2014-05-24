package com.savoirtech.hecate.cql3.naming.def;

import com.savoirtech.hecate.cql3.annotations.ColumnName;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import com.savoirtech.hecate.cql3.annotations.TableName;
import com.savoirtech.hecate.cql3.naming.NamingConvention;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;

public class DefaultNamingConvention implements NamingConvention {
//----------------------------------------------------------------------------------------------------------------------
// NamingConvention Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public String columnName(Field field) {
        ColumnName columnName = field.getAnnotation(ColumnName.class);
        return columnName == null ? field.getName() : columnName.value();
    }

    @Override
    public boolean isIdentifier(Field field) {
        return field.getAnnotation(IdColumn.class) != null;
    }

    @Override
    public String tableName(Class<?> pojoType) {
        return tableName(pojoType, pojoType.getSimpleName());
    }

    @Override
    public String tableName(Field field) {
        return tableName(field, field.getName());
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private String tableName(AnnotatedElement element, String defaultValue) {
        TableName tableName = element.getAnnotation(TableName.class);
        return tableName == null ? defaultValue : tableName.value();
    }
}
