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

import com.savoirtech.hecate.cql3.persistence.Dehydrator;

public class DefaultDehydrator extends PersistenceTaskExecutor implements Dehydrator {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultDehydrator(DefaultPersistenceContext persistenceContext) {
        super(persistenceContext);
    }

//----------------------------------------------------------------------------------------------------------------------
// Dehydrator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void dehydrate(Class<?> pojoType, String tableName, Object identifier, Object pojo) {
        enqueue(new DehydratePojoTask(pojoType, tableName, pojo));
        executeTasks();
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private final class DehydratePojoTask implements PersistenceTask {
        private final Class<?> pojoType;
        private final String tableName;
        private final Object pojo;

        public DehydratePojoTask(Class<?> pojoType, String tableName, Object pojo) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.pojo = pojo;
        }

        @Override
        public void execute(DefaultPersistenceContext persistenceContext) {
            persistenceContext.createSave(pojoType, tableName).execute(DefaultDehydrator.this, pojo);
        }
    }
}
