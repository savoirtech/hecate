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

package com.savoirtech.hecate.cql3.persistence.def;

import com.savoirtech.hecate.cql3.ReflectionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class PersistenceTaskExecutor {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final DefaultPersistenceContext persistenceContext;
    private final Queue<PersistenceTask> queue = new LinkedList<>();

    private final Set<String> visitedCache = new HashSet<>();

    private final Map<String, Object> pojoCache = new HashMap<>();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PersistenceTaskExecutor(DefaultPersistenceContext persistenceContext) {
        this.persistenceContext = persistenceContext;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void enqueue(PersistenceTask task) {
        queue.add(task);
    }

    protected void executeTasks() {
        while (!queue.isEmpty()) {
            queue.poll().execute(persistenceContext);
        }
    }

    protected boolean isVisited(Class<?> pojoType, String tableName, Object identifier) {
        final String key = visitedCacheKey(pojoType, tableName, identifier);
        if (visitedCache.contains(key)) {
            return true;
        }
        visitedCache.add(key);
        return false;
    }

    private String visitedCacheKey(Class<?> pojoType, String tableName, Object identifier) {
        return pojoType.getName() + ":" + tableName + ":" + identifier;
    }

    protected Object newPojo(Class<?> pojoType, Object identifier) {
        final String key = pojoCacheKey(pojoType, identifier);
        Object pojo = pojoCache.get(key);
        if (pojo == null) {
            pojo = ReflectionUtils.instantiate(pojoType);
            pojoCache.put(key, pojo);
        }
        return pojo;
    }

    private String pojoCacheKey(Class<?> pojoType, Object identifier) {
        return pojoType.getName() + ":" + identifier;
    }

    protected List<Object> pruneIdentifiers(Class<?> pojoType, String tableName, Iterable<Object> identifiers) {
        final List<Object> pruned = new LinkedList<>();
        for (Object identifier : identifiers) {
            final String key = visitedCacheKey(pojoType, tableName, identifier);
            if (!visitedCache.contains(key)) {
                visitedCache.add(key);
                pruned.add(identifier);
            }
        }
        return pruned;
    }
}
