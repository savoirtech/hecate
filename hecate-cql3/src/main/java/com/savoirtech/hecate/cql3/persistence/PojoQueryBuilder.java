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

package com.savoirtech.hecate.cql3.persistence;

public interface PojoQueryBuilder<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    PojoQuery<P> build();

    PojoQueryBuilder<P> identifierEquals();

    PojoQueryBuilder<P> identifierIn();

    PojoQueryBuilder<P> eq(String facetName);

    PojoQueryBuilder<P> eq(String facetName, Object value);

    PojoQueryBuilder<P> gt(String facetName);

    PojoQueryBuilder<P> gt(String facetName, Object value);

    PojoQueryBuilder<P> gte(String facetName);

    PojoQueryBuilder<P> gte(String facetName, Object value);

    PojoQueryBuilder<P> in(String facetName);

    PojoQueryBuilder<P> in(String facetName, Object value);

    PojoQueryBuilder<P> lt(String facetName);

    PojoQueryBuilder<P> lt(String facetName, Object value);

    PojoQueryBuilder<P> lte(String facetName);

    PojoQueryBuilder<P> lte(String facetName, Object value);
}
