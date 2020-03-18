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

import com.savoirtech.hecate.core.query.QueryResult;
import com.savoirtech.hecate.core.statement.StatementOptions;
import com.savoirtech.hecate.core.statement.StatementOptionsBuilder;

public interface PojoQuery<P> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    default QueryResult<P> execute(Object... params) {
        return execute(StatementOptionsBuilder.empty(), params);
    }

    QueryResult<P> execute(StatementOptions options, Object... params);

    default PojoMultiQuery<P> multi() {
        return multi(StatementOptionsBuilder.empty());
    }

    PojoMultiQuery<P> multi(StatementOptions options);
}
