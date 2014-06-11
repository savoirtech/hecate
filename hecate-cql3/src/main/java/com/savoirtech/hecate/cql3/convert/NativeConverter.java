package com.savoirtech.hecate.cql3.convert;

import com.datastax.driver.core.DataType;

public final class NativeConverter implements ValueConverter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final ValueConverter BOOLEAN = new NativeConverter(DataType.cboolean());
    public static final ValueConverter DATE = new NativeConverter(DataType.timestamp());
    public static final ValueConverter DOUBLE = new NativeConverter(DataType.cdouble());
    public static final ValueConverter FLOAT = new NativeConverter(DataType.cfloat());
    public static final ValueConverter INTEGER = new NativeConverter(DataType.cint());
    public static final ValueConverter LONG = new NativeConverter(DataType.bigint());
    public static final ValueConverter UUID = new NativeConverter(DataType.uuid());
    public static final ValueConverter STRING = new NativeConverter(DataType.varchar());
    public static final ValueConverter INET = new NativeConverter(DataType.inet());
    public static final ValueConverter BIG_DECIMAL = new NativeConverter(DataType.decimal());
    public static final ValueConverter BIG_INTEGER = new NativeConverter(DataType.varint());

    private final DataType dataType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    private NativeConverter(DataType dataType) {
        this.dataType = dataType;
    }

//----------------------------------------------------------------------------------------------------------------------
// ValueConverter Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Object fromCassandraValue(Object value) {
        return value;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public Object toCassandraValue(Object value) {
        return value;
    }
}
