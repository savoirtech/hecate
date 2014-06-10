package com.savoirtech.hecate.cql3.dao.def;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PersisterFactory;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DefaultPojoDao<K, P> implements PojoDao<K, P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersisterFactory persisterFactory;
    private final Persister rootPersister;
    private final Class<P> rootPojoType;
    private final String rootTableName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(PersisterFactory persisterFactory, Class<P> rootPojoType, String rootTableName) {
        this.persisterFactory = persisterFactory;
        this.rootPersister = persisterFactory.getPersister(rootPojoType, rootTableName);
        this.rootPojoType = rootPojoType;
        this.rootTableName = rootTableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoDao Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void delete(K key) {
//        final P pojo = findByKey(key);
//        if (pojo != null) {
//            delete.execute(pojo);
//        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public P findByKey(K key) {
        final Queue<PojoQueryItem> queryItems = new LinkedList<>();
        final QueryContext context = new QueryContextImpl(queryItems);
        P root = (P) rootPersister.findByKey().find(key, context);
        while (!queryItems.isEmpty()) {
            PojoQueryItem item = queryItems.poll();
            persisterFactory.getPersister(item.getPojoType(), item.getTableName()).findByKey().find(item.getIdentifier(), item.getPojo(), context);
        }
        return root;
    }

    @Override
    public List<P> findByKeys(Iterable<K> keys) {
//        return findByKeys.execute(keys);
        return null;
    }

    @Override
    public void save(P pojo) {
        Queue<PojoSaveItem> saveItems = new LinkedList<>();
        saveItems.add(new PojoSaveItem(rootPojoType, rootTableName, pojo));
        final SaveContext saveContext = new SaveContextImpl(saveItems);
        while (!saveItems.isEmpty()) {
            PojoSaveItem item = saveItems.poll();
            persisterFactory.getPersister(item.getPojoType(), item.getTableName()).save().execute(item.getPojo(), saveContext);
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class PojoQueryItem {
        private final Class<?> pojoType;
        private final String tableName;
        private final Object identifier;
        private final Object pojo;

        private PojoQueryItem(Class<?> pojoType, String tableName, Object identifier, Object pojo) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.identifier = identifier;
            this.pojo = pojo;
        }

        public String getTableName() {
            return tableName;
        }

        public Class<?> getPojoType() {
            return pojoType;
        }

        public Object getIdentifier() {
            return identifier;
        }

        public Object getPojo() {
            return pojo;
        }
    }

    private static class PojoSaveItem {
        private final Class<?> pojoType;
        private final String tableName;
        private final Object pojo;

        public PojoSaveItem(Class<?> pojoType, String tableName, Object pojo) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.pojo = pojo;
        }

        public Class<?> getPojoType() {
            return pojoType;
        }

        public String getTableName() {
            return tableName;
        }

        public Object getPojo() {
            return pojo;
        }
    }

    private static class QueryContextImpl implements QueryContext {
        private final Queue<PojoQueryItem> items;

        private QueryContextImpl(Queue<PojoQueryItem> items) {
            this.items = items;
        }

        @Override
        public void addPojo(Class<?> pojoType, String tableName, Object identifier, Object pojo) {
            items.add(new PojoQueryItem(pojoType, tableName, identifier, pojo));
        }
    }

    private static class SaveContextImpl implements SaveContext {
        private final Queue<PojoSaveItem> items;

        private SaveContextImpl(Queue<PojoSaveItem> items) {
            this.items = items;
        }

        @Override
        public void enqueue(Class<?> pojoType, String tableName, Object pojo) {
            items.add(new PojoSaveItem(pojoType, tableName, pojo));
        }
    }
}
