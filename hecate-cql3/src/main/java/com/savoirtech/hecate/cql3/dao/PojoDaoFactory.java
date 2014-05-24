package com.savoirtech.hecate.cql3.dao;

public interface PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType);

    <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType, String tableName);
}
