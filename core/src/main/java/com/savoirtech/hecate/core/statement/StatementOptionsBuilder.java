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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class StatementOptionsBuilder {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final StatementOptions EMPTY = new StatementOptionsBuilder().build();
    private final List<Function<Statement, Statement>> options = new LinkedList<>();

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static StatementOptionsBuilder consistencyLevel(ConsistencyLevel level) {
        return new StatementOptionsBuilder().withConsistencyLevel(level);
    }

    public static StatementOptionsBuilder defaultTimestamp(long timestamp) {
        return new StatementOptionsBuilder().withDefaultTimestamp(timestamp);
    }

    public static StatementOptionsBuilder disableTracing() {
        return new StatementOptionsBuilder().withDisableTracing();
    }

    public static StatementOptions empty() {
        return EMPTY;
    }

    public static StatementOptionsBuilder enableTracing() {
        return new StatementOptionsBuilder().withEnableTracing();
    }

    public static StatementOptionsBuilder fetchSize(int size) {
        return new StatementOptionsBuilder().withFetchSize(size);
    }

    /**
     * 4.x driver no longer supports retryPolicy settings on a statement.
     * In addition, by default, there is now only one default retry policy implementation
     */
    @Deprecated
    public static StatementOptionsBuilder retryPolicy(RetryPolicy policy) {
        return new StatementOptionsBuilder().withRetryPolicy(policy);
    }

    public static StatementOptionsBuilder serialConsistencyLevel(ConsistencyLevel level) {
        return new StatementOptionsBuilder().withSerialConsistencyLevel(level);
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    StatementOptionsBuilder() {

    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public StatementOptions build() {
        return new DefaultStatementOptions(options);
    }

    public StatementOptionsBuilder withConsistencyLevel(ConsistencyLevel level) {
        options.add(stmt -> stmt.setConsistencyLevel(level));
        return this;
    }

    private StatementOptionsBuilder withDefaultTimestamp(long timestamp) {
        options.add(stmt -> stmt.setQueryTimestamp(timestamp));
        return this;
    }

    private StatementOptionsBuilder withDisableTracing() {
        options.add(stmt -> stmt.setTracing(false));
        return this;
    }

    private StatementOptionsBuilder withEnableTracing() {
        options.add(stmt -> stmt.setTracing(true));
        return this;
    }

    private StatementOptionsBuilder withFetchSize(int size) {
        options.add(stmt -> stmt.setPageSize(size));
        return this;
    }

    private StatementOptionsBuilder withRetryPolicy(RetryPolicy policy) {
        return this;
    }

    private StatementOptionsBuilder withSerialConsistencyLevel(ConsistencyLevel level) {
        options.add(stmt -> stmt.setSerialConsistencyLevel(level));
        return this;
    }
}
