package com.savoirtech.hecate.cql3.dao.def;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.persistence.PojoDelete;
import com.savoirtech.hecate.cql3.persistence.PojoFindByKey;
import com.savoirtech.hecate.cql3.persistence.PojoFindByKeys;
import com.savoirtech.hecate.cql3.persistence.PojoSave;

import java.util.List;

public class DefaultPojoDao<K, P> implements PojoDao<K, P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoSave<P> save;
    private final PojoFindByKey<P, K> findByKey;
    private final PojoDelete<P> delete;
    private final PojoFindByKeys<P, K> findByKeys;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(Session session, String tableName, PojoDescriptor<P> descriptor) {
        this.save = new PojoSave<>(session, tableName, descriptor);
        this.findByKey = new PojoFindByKey<>(session, tableName, descriptor);
        this.delete = new PojoDelete<>(session, tableName, descriptor);
        this.findByKeys = new PojoFindByKeys<>(session, tableName, descriptor);
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDao Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public void delete(K key) {
        final P pojo = findByKey(key);
        if (pojo != null) {
            delete.execute(pojo);
        }
    }

    @Override
    public P findByKey(K key) {
        return findByKey.find(key);
    }

    @Override
    public void save(P pojo) {
        save.execute(pojo);
    }

    @Override
    public List<P> findByKeys(Iterable<K> keys) {
        return findByKeys.execute(keys);
    }
}
