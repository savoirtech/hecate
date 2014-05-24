package com.savoirtech.hecate.cql3.type.def;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;
import com.savoirtech.hecate.cql3.type.NativeType;
import org.apache.commons.lang3.ClassUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DefaultColumnTypeRegistry implements ColumnTypeRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private Map<Class<?>,ColumnType<?>> columnTypes = new HashMap<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultColumnTypeRegistry() {
        addColumnType(Boolean.class, new NativeType<Boolean>(DataType.cboolean()));
        addColumnType(Date.class, new NativeType<Date>(DataType.timestamp()));
        addColumnType(Double.class, new NativeType<Double>(DataType.cdouble()));
        addColumnType(Float.class, new NativeType<Float>(DataType.cfloat()));
        addColumnType(Integer.class, new NativeType<Integer>(DataType.cint()));
        addColumnType(Long.class, new NativeType<Long>(DataType.bigint()));
        addColumnType(UUID.class, new NativeType<UUID>(DataType.uuid()));
        addColumnType(String.class, new NativeType<String>(DataType.varchar()));
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnTypeRegistry Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <T> ColumnType<T> getColumnType(Class<T> type) {
        return (ColumnType<T>)columnTypes.get(type);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public final <T> void addColumnType(Class<T> javaType, ColumnType<T> columnType) {
        columnTypes.put(javaType, columnType);
        if(ClassUtils.isPrimitiveWrapper(javaType)) {
            columnTypes.put(ClassUtils.wrapperToPrimitive(javaType), columnType);
        }
    }
}
