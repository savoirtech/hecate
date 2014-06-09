package com.savoirtech.hecate.cql3.convert;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.Hydrator;

public interface ValueConverter {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Object fromCassandraValue(Object value, Hydrator hydrator);

    DataType getDataType();

    Object toCassandraValue(Object value, Dehydrator dehydrator);
}
