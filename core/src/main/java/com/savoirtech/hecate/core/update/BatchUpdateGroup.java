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

package com.savoirtech.hecate.core.update;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

public class BatchUpdateGroup implements UpdateGroup {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final CqlSession session;
    private BatchStatement batchStatement;
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public BatchUpdateGroup(CqlSession session, Executor executor) {
        this(session, BatchStatement.newInstance(DefaultBatchType.LOGGED), executor);
    }

    public BatchUpdateGroup(CqlSession session, BatchStatement batchStatement, Executor executor) {
        this.session = session;
        this.batchStatement = batchStatement;
        this.executor = executor;
    }

    public BatchUpdateGroup(CqlSession session, BatchType batchType, Executor executor) {
        this(session, BatchStatement.newInstance(batchType), executor);
    }

//----------------------------------------------------------------------------------------------------------------------
// UpdateGroup Implementation
//----------------------------------------------------------------------------------------------------------------------

    public void addUpdate(Statement statement) {
        try {
            BatchableStatement batchableStatement = (BatchableStatement) statement;
            batchStatement = batchStatement.add(batchableStatement);
        } catch (Exception e) {
            throw new RuntimeException("Invalid statement type for BatchUpdateGroup.addUpdate: " + statement.getClass());
        }
    }

    @Override
    public void complete() {
        session.execute(batchStatement);
    }

    @Override
    public CompletableFuture<Void> completeAsync() {
        return session.executeAsync(batchStatement).thenApplyAsync((Function<AsyncResultSet, Void>) input -> null, executor).toCompletableFuture();
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public BatchStatement getBatchStatement() {
        return batchStatement;
    }
}
