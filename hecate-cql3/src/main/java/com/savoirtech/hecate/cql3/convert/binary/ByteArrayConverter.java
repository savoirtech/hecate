package com.savoirtech.hecate.cql3.convert.binary;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;

import java.nio.ByteBuffer;

public class ByteArrayConverter implements ValueConverter {

    @Override
    public Object fromCassandraValue(Object value) {
        if(value == null) {
            return null;
        }
        ByteBuffer buff = (ByteBuffer)value;
        byte[] bytes = new byte[buff.remaining()];
        buff.get(bytes);
        return bytes;
    }

    @Override
    public DataType getDataType() {
        return DataType.blob();
    }

    @Override
    public Object toCassandraValue(Object value) {
        return value == null ? null : ByteBuffer.wrap((byte[])value);
    }
}
