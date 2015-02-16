/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.cql3.util;

public class PojoCacheKey {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> pojoType;
    private final String tableName;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoCacheKey(Class<?> pojoType, String tableName) {
        this.pojoType = pojoType;
        this.tableName = tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Class<?> getPojoType() {
        return pojoType;
    }

    public String getTableName() {
        return tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PojoCacheKey that = (PojoCacheKey) o;

        if (!pojoType.equals(that.pojoType)) {
            return false;
        }
        return !(tableName != null ? !tableName.equals(that.tableName) : that.tableName != null);
    }

    @Override
    public int hashCode() {
        int result = pojoType.hashCode();
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        return result;
    }
}
