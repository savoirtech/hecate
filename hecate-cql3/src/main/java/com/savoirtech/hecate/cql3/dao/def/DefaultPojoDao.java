package com.savoirtech.hecate.cql3.dao.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.persistence.PojoInsert;

public class DefaultPojoDao<K,P> implements PojoDao<K,P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoInsert<P> insert;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(Session session, String tableName, PojoDescriptor<P> descriptor) {
        this.insert = new PojoInsert<>(session, tableName, descriptor); 
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDao Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public P findByKey(K key) {
        return null;
    }

    @Override
    public void save(P pojo) {
        insert.execute(pojo);
    }
}
