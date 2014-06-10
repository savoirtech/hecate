package com.savoirtech.hecate.cql3.persistence;

public interface Persister {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    PojoDelete delete();

    PojoFindByKey findByKey();

    PojoFindByKeys findByKeys();

    PojoFindForDelete findForDelete();

    PojoSave save();
}
