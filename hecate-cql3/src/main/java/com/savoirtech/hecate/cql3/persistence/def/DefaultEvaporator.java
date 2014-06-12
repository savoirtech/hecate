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

import com.savoirtech.hecate.cql3.persistence.Evaporator;

import java.util.List;

public class DefaultEvaporator extends PersistenceTaskExecutor implements Evaporator {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultEvaporator(DefaultPersistenceContext persistenceContext) {
        super(persistenceContext);
    }

//----------------------------------------------------------------------------------------------------------------------
// Evaporator Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void evaporate(Class<?> pojoType, String tableName, Iterable<Object> identifiers) {
        List<Object> pruned = pruneIdentifiers(pojoType, tableName, identifiers);
        if (!pruned.isEmpty()) {
            enqueue(new DeletePojosTask(pojoType, tableName, pruned));
        }
        executeTasks();
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private final class DeletePojosTask implements PersistenceTask {
        private final Class<?> pojoType;
        private final String tableName;
        private final Iterable<Object> identifiers;

        private DeletePojosTask(Class<?> pojoType, String tableName, Iterable<Object> identifiers) {
            this.pojoType = pojoType;
            this.tableName = tableName;
            this.identifiers = identifiers;
        }

        @Override
        public void execute(DefaultPersistenceContext context) {
            context.delete(pojoType, tableName).execute(DefaultEvaporator.this, identifiers);
        }
    }
}
