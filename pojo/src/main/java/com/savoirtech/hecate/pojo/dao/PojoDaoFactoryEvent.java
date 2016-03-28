/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.dao;

import com.savoirtech.hecate.pojo.binding.PojoBinding;

public class PojoDaoFactoryEvent<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private PojoDaoFactory factory;
    private final PojoDao<P> pojoDao;
    private final PojoBinding<P> pojoBinding;
    private final String tableName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDaoFactoryEvent(PojoDao<P> pojoDao, PojoBinding<P> pojoBinding, String tableName) {
        this.pojoDao = pojoDao;
        this.pojoBinding = pojoBinding;
        this.tableName = tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public PojoDaoFactory getFactory() {
        return factory;
    }

    public PojoBinding<P> getPojoBinding() {
        return pojoBinding;
    }

    public PojoDao<P> getPojoDao() {
        return pojoDao;
    }

    public String getTableName() {
        return tableName;
    }
}
