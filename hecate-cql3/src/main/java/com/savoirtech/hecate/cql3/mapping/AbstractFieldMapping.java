package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import org.apache.commons.lang3.Validate;

import java.lang.reflect.Field;

public abstract class AbstractFieldMapping implements FieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Field field;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AbstractFieldMapping(Field field) {
        this.field = Validate.notNull(field);
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object fieldCassandraValue(Object pojo) {
        return rawCassandraValue(getFieldValue(pojo));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected Class<?> getFieldType() {
        return field.getType();
    }

    protected Object getFieldValue(Object root) {
        return ReflectionUtils.getFieldValue(field, root);
    }

    protected void setFieldValue(Object root, Object fieldValue) {
        ReflectionUtils.setFieldValue(field, root, fieldValue);
    }
}
