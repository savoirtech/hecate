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

package com.savoirtech.hecate.pojo.query;

public interface PojoQueryBuilder<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Sorts the query by the facet, ascending.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> asc(String facetName);

    /**
     * Builds the query.
     *
     * @return the query
     */
    PojoQuery<P> build();

    /**
     * Sorts the query by the facet, descending.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> desc(String facetName);

    /**
     * Adds a parameterized "=" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> eq(String facetName);

    /**
     * Adds a constant-valued "=" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @param value     the constant value
     * @return this builder
     */
    PojoQueryBuilder<P> eq(String facetName, Object value);

    /**
     * Adds a parameterized "&gt;" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> gt(String facetName);

    /**
     * Adds a constant-valued "&gt;" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @param value     the constant value
     * @return this builder
     */
    PojoQueryBuilder<P> gt(String facetName, Object value);

    /**
     * Adds a parameterized "&gt;=" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> gte(String facetName);

    /**
     * Adds a constant-valued "&gt;=" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @param value     the constant value
     * @return this builder
     */
    PojoQueryBuilder<P> gte(String facetName, Object value);

    /**
     * Adds a parameterized "IN" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> in(String facetName);

    /**
     * Adds a constant-valued "IN" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @param values    the constant value
     * @return this builder
     */
    PojoQueryBuilder<P> in(String facetName, Iterable<Object> values);

    /**
     * Adds a parameterized "&lt;" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> lt(String facetName);

    /**
     * Adds a constant-valued "&lt;" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @param value     the constant value
     * @return this builder
     */
    PojoQueryBuilder<P> lt(String facetName, Object value);

    /**
     * Adds a parameterized "&lt;=" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @return this builder
     */
    PojoQueryBuilder<P> lte(String facetName);

    /**
     * Adds a constant-valued "&lt;=" clause to the query for the facet.
     *
     * @param facetName the facet name
     * @param value     the constant value
     * @return this builder
     */
    PojoQueryBuilder<P> lte(String facetName, Object value);
}
