package com.savoirtech.hecate.cql3.persistence;

public interface Persister {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    PojoDelete delete();

    PojoFindByKey findByKey();

    PojoSave save();

    PojoFindByKeys findByKeys();
}
