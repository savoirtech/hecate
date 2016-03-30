/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

public interface PojoDaoFactory {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Adds a new {@link PojoDaoFactoryListener} to this factory.
     *
     * @param listener the listener
     * @return this factory
     */
    PojoDaoFactory addListener(PojoDaoFactoryListener listener);

    /**
     * Creates a {@link PojoDao} for {@link P} objects within the default table name (defined by the {@link com.savoirtech.hecate.pojo.naming.NamingStrategy}
     *
     * @param pojoType the POJO type
     * @param <P>      the POJO type parameter
     * @return the DAO
     * @see com.savoirtech.hecate.pojo.naming.NamingStrategy#getTableName(Class)
     */
    <P> PojoDao<P> createPojoDao(Class<P> pojoType);

    /**
     * Creates a {@link PojoDao} for {@link P} objects in the specified table.
     *
     * @param pojoType  the POJO type
     * @param tableName the table name
     * @param <P>       the POJO type parameter
     * @return the DAO
     */
    <P> PojoDao<P> createPojoDao(Class<P> pojoType, String tableName);
}
