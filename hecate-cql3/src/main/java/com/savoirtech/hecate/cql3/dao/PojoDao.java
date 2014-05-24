package com.savoirtech.hecate.cql3.dao;

import java.util.List;

public interface PojoDao<K, P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void delete(K key);

    P findByKey(K key);

    List<P> findByKeys(Iterable<K> keys);

    void save(P pojo);
}
