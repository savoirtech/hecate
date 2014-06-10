package com.savoirtech.hecate.cql3.persistence.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PojoFindByKey;
import com.savoirtech.hecate.cql3.persistence.PojoSave;

public class DefaultPersister implements Persister {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoSave save;
    private final PojoFindByKey findByKey;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersister(Session session, PojoMapping mapping) {
        this.save = new PojoSave(session, mapping);
        this.findByKey = new PojoFindByKey(session, mapping);
    }

    @Override
    public PojoFindByKey findByKey() {
        return findByKey;
    }

    @Override
    public PojoSave save() {
        return save;
    }
}
