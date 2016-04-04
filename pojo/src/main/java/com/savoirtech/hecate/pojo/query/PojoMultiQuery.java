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

import com.savoirtech.hecate.core.mapping.MappedQueryResult;
import com.savoirtech.hecate.core.query.QueryResult;

public interface PojoMultiQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Executes a new query using the supplied parameters.
     *
     * @param params the parameters
     * @return this query
     */
    PojoMultiQuery<P> add(Object... params);

    /**
     * Returns a {@link MappedQueryResult} which iterates through all {@link P} objects found by all queries executed
     * by this query.
     *
     * @return the query result(s)
     */
    QueryResult<P> execute();
}
