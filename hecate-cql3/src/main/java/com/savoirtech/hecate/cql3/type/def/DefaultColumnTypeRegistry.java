package com.savoirtech.hecate.cql3.type.def;

import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;
import com.savoirtech.hecate.cql3.type.natives.BooleanType;
import com.savoirtech.hecate.cql3.type.natives.DateType;
import com.savoirtech.hecate.cql3.type.natives.DoubleType;
import com.savoirtech.hecate.cql3.type.natives.FloatType;
import com.savoirtech.hecate.cql3.type.natives.IntegerType;
import com.savoirtech.hecate.cql3.type.natives.LongType;
import com.savoirtech.hecate.cql3.type.natives.UuidType;
import com.savoirtech.hecate.cql3.type.natives.VarcharType;
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
        addColumnType(Boolean.class, new BooleanType());
        addColumnType(Date.class, new DateType());
        addColumnType(Double.class, new DoubleType());
        addColumnType(Float.class, new FloatType());
        addColumnType(Integer.class, new IntegerType());
        addColumnType(Long.class, new LongType());
        addColumnType(UUID.class, new UuidType());
        addColumnType(String.class, new VarcharType());
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
