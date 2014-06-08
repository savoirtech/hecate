package com.savoirtech.hecate.cql3.convert;

import com.datastax.driver.core.DataType;

public interface ValueConverter {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Object fromCassandraValue(Object value);

    DataType getDataType();

    Object toCassandraValue(Object value);
}
