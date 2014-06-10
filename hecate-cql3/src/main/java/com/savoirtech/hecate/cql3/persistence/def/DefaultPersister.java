package com.savoirtech.hecate.cql3.persistence.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PojoDelete;
import com.savoirtech.hecate.cql3.persistence.PojoFindByKey;
import com.savoirtech.hecate.cql3.persistence.PojoFindByKeys;
import com.savoirtech.hecate.cql3.persistence.PojoSave;

public class DefaultPersister implements Persister {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoSave save;
    private final PojoFindByKey findByKey;
    private final PojoFindByKeys findByKeys;
    private final PojoDelete delete;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersister(Session session, PojoMapping mapping) {
        this.save = new PojoSave(session, mapping);
        this.findByKey = new PojoFindByKey(session, mapping);
        this.findByKeys = new PojoFindByKeys(session, mapping);
        this.delete = new PojoDelete(session, mapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// Persister Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoDelete delete() {
        return delete;
    }

    @Override
    public PojoFindByKey findByKey() {
        return findByKey;
    }

    @Override
    public PojoFindByKeys findByKeys() {
        return findByKeys;
    }

    @Override
    public PojoSave save() {
        return save;
    }
}
