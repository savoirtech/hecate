/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.dao.def;

import com.savoirtech.hecate.cql3.dao.PojoDao;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PersisterFactory;
import com.savoirtech.hecate.cql3.persistence.PojoFindForDelete;
import com.savoirtech.hecate.cql3.persistence.PojoQuery;
import com.savoirtech.hecate.cql3.persistence.def.DefaultPersistenceContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class DefaultPojoDao<K, P> implements PojoDao<K, P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PersisterFactory persisterFactory;
    private final DefaultPersistenceContext persistenceContext;
    private final Persister rootPersister;
    private final Class<P> rootPojoType;
    private final String rootTableName;
    private final PojoQuery<P> findByKey;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoDao(PersisterFactory persisterFactory, DefaultPersistenceContext persistenceContext, Class<P> rootPojoType, String rootTableName) {
        this.persisterFactory = persisterFactory;
        this.persistenceContext = persistenceContext;
        this.rootPersister = persisterFactory.getPersister(rootPojoType, rootTableName);
        this.rootPojoType = rootPojoType;
        this.rootTableName = rootTableName;
        this.findByKey = persistenceContext.find(rootPojoType, rootTableName).identifierEquals().build();
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
        return persistenceContext.findByKey(rootPojoType, rootTableName).execute(persistenceContext.newHydrator(), key).one();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<P> findByKeys(Iterable<K> keys) {
        return persistenceContext.findByKeys(rootPojoType, rootTableName).execute(persistenceContext.newHydrator(), keys).list();
    }

    @Override
    public void save(P pojo) {
        persistenceContext.createSave(rootPojoType, rootTableName).execute(pojo);
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

    private final class DeleteContextImpl implements DeleteContext {
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

    private final class DeletePojosTask implements PersistenceTask {
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

    private interface PersistenceTask {
        void execute();
    }

    private final class VisitedPojoCache {
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
