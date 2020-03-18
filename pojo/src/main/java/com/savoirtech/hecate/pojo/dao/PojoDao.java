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

import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.update.UpdateGroup;
import com.savoirtech.hecate.pojo.query.PojoMultiQuery;
import com.savoirtech.hecate.pojo.query.PojoQuery;
import com.savoirtech.hecate.pojo.query.PojoQueryBuilder;
import java.util.function.Function;

public interface PojoDao<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Deletes a POJO.
     *
     * @param pojo the POJO
     */
    void delete(P pojo);

    /**
     * Deletes a POJO using the supplied {@link StatementOptions}.
     *
     * @param options the statement options
     * @param pojo    the POJO
     */
    void delete(StatementOptions options, P pojo);

    /**
     * Deletes a POJO using the supplied {@link UpdateGroup}.
     *
     * @param group the update group
     * @param pojo  the POJO
     */
    void delete(UpdateGroup group, P pojo);

    /**
     * Deletes a POJO using the supplied {@link StatementOptions} within the supplied {@link UpdateGroup}.
     *
     * @param group   the update group
     * @param options the statement options
     * @param pojo    the POJO
     */
    void delete(UpdateGroup group, StatementOptions options, P pojo);

    /**
     * Asynchronously deletes a POJO.
     *
     * @param pojo the POJO
     * @return a future which will be completed when the POJO is deleted
     */
    CompletableFuture<Void> deleteAsync(P pojo);

    /**
     * Asynchronously deletes a POJO using the supplied {@link StatementOptions}.
     *
     * @param options the statement options
     * @param pojo    the POJO
     * @return a future which will be completed when the POJO is deleted
     */
    CompletableFuture<Void> deleteAsync(StatementOptions options, P pojo);

    /**
     * Deletes a POJO by its primary key.
     *
     * @param components the components of the primary key
     */
    void deleteByKey(Object... components);

    /**
     * Deletes a POJO by its primary key using the supplied {@link StatementOptions}.
     *
     * @param options    the statement options
     * @param components the components of the primary key
     */
    void deleteByKey(StatementOptions options, Object... components);

    /**
     * Deletes a POJO by its primary key using the supplied (@link StatmentOptions} and within the supplied
     * {@link UpdateGroup}.
     *
     * @param group      the update group
     * @param options    the statement options
     * @param components the components of the primary key
     */
    void deleteByKey(UpdateGroup group, StatementOptions options, Object... components);

    /**
     * Asynchronously deletes a POJO by its primary key.
     *
     * @param components the components of the primary key
     * @return a future which will be completed when the POJO is deleted
     */
    CompletableFuture<Void> deleteByKeyAsync(Object... components);

    /**
     * Asynchronously deletes a POJO by its primary key using the supplied {@link StatementOptions}.
     *
     * @param options    the statement options
     * @param components the components of the primary key
     * @return a future which will be completed when the POJO is deleted
     */
    CompletableFuture<Void> deleteByKeyAsync(StatementOptions options, Object... components);

    /**
     * Returns a {@link PojoQueryBuilder} which can be used to build a query to find {@link P} objects.
     *
     * @return the query builder
     */
    PojoQueryBuilder<P> find();

    /**
     * Returns a {@link PojoQuery} which uses a custom where clause to find {@link P} objects.
     *
     * @param builder a function which builds a custom where clause
     * @return the query
     */
    PojoQuery<P> find(Function<Select, Select> builder);

    /**
     * Finds an POJO by its primary key.
     *
     * @param components the components of the primary key
     * @return the POJO corresponding to the primary key
     */
    P findByKey(Object... components);


    /**
     * Finds an POJO by its primary key, using the provided {@link StatementOptions}.
     *
     * @param options    the statement options
     * @param components the components of the primary key
     * @return the POJO corresponding to the primary key
     */
    P findByKey(StatementOptions options, Object... components);

    /**
     * Returns a {@link PojoMultiQuery} which can be used to search for multiple entities by their primary keys.
     *
     * @return the query
     */
    PojoMultiQuery<P> findByKeys();

    /**
     * Returns a {@link PojoMultiQuery} which can be used to search for multiple entities by their primary keys, using
     * the provided {@link StatementOptions} for each query.
     *
     * @param options the statement options
     * @return the query
     */
    PojoMultiQuery<P> findByKeys(StatementOptions options);

    /**
     * Saves a POJO.
     *
     * @param pojo the POJO
     */
    void save(P pojo);

    /**
     * Saves a POJO using the supplied TTL value.
     *
     * @param pojo the POJO
     * @param ttl  the TTL
     */
    void save(P pojo, int ttl);

    /**
     * Saves a POJO using the supplied {@link StatementOptions}.
     *
     * @param options the statement options
     * @param pojo    the POJO
     */
    void save(StatementOptions options, P pojo);

    /**
     * Saves a POJO within the supplied {@link UpdateGroup}.
     *
     * @param group the update group
     * @param pojo  the POJO
     */
    void save(UpdateGroup group, P pojo);

    /**
     * Saves a POJO using the supplied {@link StatementOptions} and TTL value.
     *
     * @param options the statement options
     * @param pojo    the POJO
     * @param ttl     the TTL
     */
    void save(StatementOptions options, P pojo, int ttl);

    /**
     * Saves a POJO within the supplied {@link UpdateGroup} with the supplied TTL value.
     *
     * @param group the update group
     * @param pojo  the POJO
     * @param ttl   the TTL
     */
    void save(UpdateGroup group, P pojo, int ttl);

    /**
     * Saves a POJO using the supplied {@link StatementOptions} and within the supplied {@link UpdateGroup}.
     *
     * @param group   the update group
     * @param options the statement options
     * @param pojo    the POJO
     */
    void save(UpdateGroup group, StatementOptions options, P pojo);

    /**
     * Saves a POJO using the supplied {@link StatementOptions}, within the supplied {@link UpdateGroup}, and using
     * the supplied TTL value.
     *
     * @param group   the update group
     * @param options the statement options
     * @param pojo    the POJO
     * @param ttl     the TTL
     */
    void save(UpdateGroup group, StatementOptions options, P pojo, int ttl);

    /**
     * Asynchronously saves a POJO.
     *
     * @param pojo the POJO
     * @return a future which will be completed when the POJO is saved
     */
    CompletableFuture<Void> saveAsync(P pojo);

    /**
     * Asynchronously saves a POJO using the supplied TTL value.
     *
     * @param pojo the POJO
     * @param ttl  the TTL
     * @return a future which will be completed when the POJO is saved
     */
    CompletableFuture<Void> saveAsync(P pojo, int ttl);

    /**
     * Asynchronously saves a POJO using the supplied {@link StatementOptions}.
     *
     * @param options the statement options
     * @param pojo    the POJO
     * @return a future which will be completed when the POJO is saved
     */
    CompletableFuture<Void> saveAsync(StatementOptions options, P pojo);

    /**
     * Asynchronously saves a POJO using the supplied {@link StatementOptions} and TTL value.
     *
     * @param options the statement options
     * @param pojo    the POJO
     * @param ttl     the TTL
     * @return a future which will be completed when the POJO is saved
     */
    CompletableFuture<Void> saveAsync(StatementOptions options, P pojo, int ttl);
}
