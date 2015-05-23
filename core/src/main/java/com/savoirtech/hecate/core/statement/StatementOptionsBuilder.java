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

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class StatementOptionsBuilder {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final List<Consumer<Statement>> options  = new LinkedList<>();

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

    public static StatementOptionsBuilder enableTracing() {
        return new StatementOptionsBuilder().withEnableTracing();
    }
    
    public static StatementOptionsBuilder fetchSize(int size) {
        return new StatementOptionsBuilder().withFetchSize(size);
    }
    
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
        return statement -> options.forEach(option -> option.accept(statement));
    }

    public StatementOptionsBuilder withConsistencyLevel(ConsistencyLevel level) {
        options.add(stmt -> stmt.setConsistencyLevel(level));
        return this;
    }

    private StatementOptionsBuilder withDefaultTimestamp(long timestamp) {
        options.add(stmt -> stmt.setDefaultTimestamp(timestamp));
        return this;
    }

    private StatementOptionsBuilder withDisableTracing() {
        options.add(Statement::disableTracing);
        return this;
    }

    private StatementOptionsBuilder withEnableTracing() {
        options.add(Statement::enableTracing);
        return this;
    }

    private StatementOptionsBuilder withFetchSize(int size) {
        options.add(stmt -> stmt.setFetchSize(size));
        return this;
    }

    private StatementOptionsBuilder withRetryPolicy(RetryPolicy policy) {
        options.add(stmt -> stmt.setRetryPolicy(policy));
        return this;
    }

    private StatementOptionsBuilder withSerialConsistencyLevel(ConsistencyLevel level) {
        options.add(stmt -> stmt.setSerialConsistencyLevel(level));
        return this;
    }
}
