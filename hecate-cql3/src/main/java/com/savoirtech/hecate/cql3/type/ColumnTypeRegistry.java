package com.savoirtech.hecate.cql3.type;

public interface ColumnTypeRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    <T> ColumnType<T> getColumnType(Class<T> type);
}
