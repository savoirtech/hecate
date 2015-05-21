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

package com.savoirtech.hecate.core.statement;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;

import java.util.function.Consumer;

public class StatementModifiers {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static Consumer<Statement> consistencyLevel(ConsistencyLevel consistencyLevel) {
        return stmt -> stmt.setConsistencyLevel(consistencyLevel);
    }

    public static Consumer<Statement> defaultTimestamp(long defaultTimestamp) {
        return stmt -> stmt.setDefaultTimestamp(defaultTimestamp);
    }

    public static Consumer<Statement> disableTracing() {
        return Statement::disableTracing;
    }

    public static Consumer<Statement> enableTracing() {
        return Statement::enableTracing;
    }

    public static Consumer<Statement> fetchSize(int fetchSize) {
        return stmt -> stmt.setFetchSize(fetchSize);
    }

    public static Consumer<Statement> retryPolicy(RetryPolicy retryPolicy) {
        return stmt -> stmt.setRetryPolicy(retryPolicy);
    }

    public static Consumer<Statement> serialConsistencyLevel(ConsistencyLevel consistencyLevel) {
        return stmt -> stmt.setSerialConsistencyLevel(consistencyLevel);
    }
}
