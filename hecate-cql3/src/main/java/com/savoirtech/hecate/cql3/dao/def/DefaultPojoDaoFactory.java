package com.savoirtech.hecate.cql3.dao.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.cql3.persistence.PersisterFactory;
import com.savoirtech.hecate.cql3.persistence.def.DefaultPersisterFactory;

public class DefaultPojoDaoFactory implements PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersisterFactory persisterFactory;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDaoFactory(Session session) {
        this.persisterFactory = new DefaultPersisterFactory(session);
    }

    public DefaultPojoDaoFactory(PersisterFactory persisterFactory) {
        this.persisterFactory = persisterFactory;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDaoFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType) {
        return createPojoDao(pojoType, null);
    }

    @Override
    public <K, P> PojoDao<K, P> createPojoDao(Class<P> pojoType, String tableName) {
        return new DefaultPojoDao<>(persisterFactory, pojoType, tableName);
    }
}
