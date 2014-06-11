/*
 * Copyright (c) 2014. Savoir Technologies
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

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.persistence.Persister;
import com.savoirtech.hecate.cql3.persistence.PojoDelete;
import com.savoirtech.hecate.cql3.persistence.PojoFindByKey;
import com.savoirtech.hecate.cql3.persistence.PojoFindByKeys;
import com.savoirtech.hecate.cql3.persistence.PojoFindForDelete;
import com.savoirtech.hecate.cql3.persistence.PojoSave;

public class DefaultPersister implements Persister {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PojoSave save;
    private final PojoFindByKey findByKey;
    private final PojoFindByKeys findByKeys;
    private final PojoDelete delete;
    private final PojoFindForDelete findForDelete;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPersister(Session session, PojoMapping mapping) {
        this.save = new PojoSave(session, mapping);
        this.findByKey = new PojoFindByKey(session, mapping);
        this.findByKeys = new PojoFindByKeys(session, mapping);
        this.delete = new PojoDelete(session, mapping);
        this.findForDelete = mapping.isCascading() ? new PojoFindForDelete(session, mapping) : null;
    }

//----------------------------------------------------------------------------------------------------------------------
// Persister Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoDelete delete() {
        return delete;
    }

    @Override
    public PojoFindByKey findByKey() {
        return findByKey;
    }

    @Override
    public PojoFindByKeys findByKeys() {
        return findByKeys;
    }

    @Override
    public PojoSave save() {
        return save;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoFindForDelete findForDelete() {
        return findForDelete;
    }
}
