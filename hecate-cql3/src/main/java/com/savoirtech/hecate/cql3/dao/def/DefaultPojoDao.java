package com.savoirtech.hecate.cql3.dao.def;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PersisterFactory;
import com.savoirtech.hecate.cql3.persistence.PojoFindForDelete;
import com.savoirtech.hecate.cql3.util.InjectionTarget;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

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
        final Queue<PersistenceTask> tasks = new LinkedList<>();
        final DeleteContext context = new DeleteContextImpl(tasks);
        context.addDeletedIdentifiers(rootPojoType, rootTableName, Arrays.<Object>asList(key));
        executeTasks(tasks);
    }

    @Override
    @SuppressWarnings("unchecked")
    public P findByKey(K key) {
        final Queue<PersistenceTask> tasks = new LinkedList<>();
        P root = (P) rootPersister.findByKey().find(key, new QueryContextImpl(tasks));
        executeTasks(tasks);
        return root;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<P> findByKeys(Iterable<K> keys) {
        final Queue<PersistenceTask> tasks = new LinkedList<>();
        List<P> roots = (List<P>) rootPersister.findByKeys().execute((Iterable<Object>) keys, new QueryContextImpl(tasks));
        executeTasks(tasks);
        return roots;
    }

    @Override
    public void save(P pojo) {
        Queue<PersistenceTask> tasks = new LinkedList<>();
        tasks.add(new PojoSaveTask(rootPojoType, rootTableName, pojo, new SaveContextImpl(tasks)));
        executeTasks(tasks);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private String cacheKey(Class<?> pojoType, String tableName, Object identifier) {
        return pojoType.getName() + ":" + tableName + ":" + identifier;
    }

    private void executeTasks(Queue<PersistenceTask> taskQueue) {
        while (!taskQueue.isEmpty()) {
            PersistenceTask task = taskQueue.poll();
            task.execute();
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class DeleteContextImpl implements DeleteContext {
        private final Queue<PersistenceTask> tasks;
        private final VisitedPojoCache cache = new VisitedPojoCache();

        private DeleteContextImpl(Queue<PersistenceTask> tasks) {
            this.tasks = tasks;
        }

        @Override
        public void addDeletedIdentifiers(Class<?> pojoType, String tableName, Iterable<Object> identifiers) {
            Set<Object> pruned = cache.prune(pojoType, tableName, identifiers);
            if (!pruned.isEmpty()) {
                tasks.add(new DeletePojosTask(pojoType, tableName, pruned, this));
            }
        }
    }

    private class DeletePojosTask implements PersistenceTask {
        private final Class<?> pojoType;
        private final String tableName;
        private final Iterable<Object> identifiers;
        private final DeleteContext context;

        private DeletePojosTask(Class<?> pojoType, String tableName, Iterable<Object> identifiers, DeleteContext context) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.identifiers = identifiers;
            this.context = context;
        }

        @Override
        public void execute() {
            final Persister persister = persisterFactory.getPersister(pojoType, tableName);
            final PojoFindForDelete findForDelete = persister.findForDelete();
            if (findForDelete != null) {
                findForDelete.execute(identifiers, context);
            }
            persister.delete().execute(identifiers);
        }
    }

    private class HydratePojosTask implements PersistenceTask {
        private final Class<?> pojoType;
        private final String tableName;
        private final Iterable<Object> identifiers;
        private final QueryContext context;
        private final InjectionTarget<List<Object>> target;

        private HydratePojosTask(Class<?> pojoType, String tableName, Iterable<Object> identifiers, InjectionTarget<List<Object>> target, QueryContext context) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.identifiers = identifiers;
            this.context = context;
            this.target = target;
        }

        @Override
        public void execute() {
            final List<Object> pojos = persisterFactory.getPersister(pojoType, tableName).findByKeys().execute(identifiers, context);
            target.inject(pojos);
        }
    }

    private interface PersistenceTask {
        void execute();
    }

    private class PojoSaveTask implements PersistenceTask {
        private final SaveContext context;
        private final Class<?> pojoType;
        private final String tableName;
        private final Object pojo;

        public PojoSaveTask(Class<?> pojoType, String tableName, Object pojo, SaveContext context) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.pojo = pojo;
            this.context = context;
        }

        @Override
        public void execute() {
            persisterFactory.getPersister(pojoType, tableName).save().execute(pojo, context);
        }
    }

    private class QueryContextImpl implements QueryContext {
        private final Queue<PersistenceTask> items;
        private final VisitedPojoCache cache = new VisitedPojoCache();
        private final Map<String, Object> pojoCache = new HashMap<>();


        private QueryContextImpl(Queue<PersistenceTask> items) {
            this.items = items;
        }

        @Override
        public void addPojos(Class<?> pojoType, String tableName, Iterable<Object> identifiers, InjectionTarget<List<Object>> target) {
            if (identifiers.iterator().hasNext()) {
                items.add(new HydratePojosTask(pojoType, tableName, identifiers, target, this));
            }
        }

        @Override
        public Map<Object, Object> newPojoMap(PojoMetadata pojoMetadata, Iterable<Object> identifiers) {
            Map<Object, Object> result = new HashMap<>();
            for (Object identifier : identifiers) {
                final String key = pojoCacheKey(pojoMetadata.getPojoType(), identifier);
                Object pojo = pojoCache.get(key);
                if (pojo == null) {
                    pojo = pojoMetadata.newPojo(identifier);
                    pojoCache.put(key, pojo);
                }
                result.put(identifier, pojo);
            }
            return result;
        }

        private String pojoCacheKey(Class<?> pojoType, Object identifier) {
            return pojoType.getCanonicalName() + ":" + identifier;
        }
    }

    private class SaveContextImpl implements SaveContext {
        private final Queue<PersistenceTask> items;
        private final VisitedPojoCache cache = new VisitedPojoCache();

        private SaveContextImpl(Queue<PersistenceTask> items) {
            this.items = items;
        }

        @Override
        public void addPojo(Class<?> pojoType, String tableName, Object identifier, Object pojo) {
            if (!cache.contains(pojoType, tableName, identifier)) {
                items.add(new PojoSaveTask(pojoType, tableName, pojo, this));
            }
        }
    }

    private class VisitedPojoCache {
        private final Set<String> visited = new HashSet<>();

        public boolean contains(Class<?> pojoType, String tableName, Object identifier) {
            final String key = cacheKey(pojoType, tableName, identifier);
            if (visited.contains(key)) {
                return true;
            }
            visited.add(key);
            return false;
        }

        public Set<Object> prune(Class<?> pojoType, String tableName, Iterable<Object> identifiers) {
            final Set<Object> pruned = new HashSet<>();
            for (Object identifier : identifiers) {
                final String key = cacheKey(pojoType, tableName, identifier);
                if (!visited.contains(key)) {
                    visited.add(key);
                    pruned.add(identifier);
                }
            }
            return pruned;
        }
    }
}
