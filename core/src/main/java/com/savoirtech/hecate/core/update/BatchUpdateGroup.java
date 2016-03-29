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

import java.util.concurrent.Executor;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class BatchUpdateGroup implements UpdateGroup {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private final BatchStatement batchStatement;
    private final Executor executor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public BatchUpdateGroup(Session session, Executor executor) {
        this(session, new BatchStatement(BatchStatement.Type.LOGGED), executor);
    }

    public BatchUpdateGroup(Session session, BatchStatement batchStatement, Executor executor) {
        this.session = session;
        this.batchStatement = batchStatement;
        this.executor = executor;
    }

    public BatchUpdateGroup(Session session, BatchStatement.Type batchType, Executor executor) {
        this(session, new BatchStatement(batchType), executor);
    }

//----------------------------------------------------------------------------------------------------------------------
// UpdateGroup Implementation
//----------------------------------------------------------------------------------------------------------------------

    public void addUpdate(Statement statement) {
        batchStatement.add(statement);
    }

    @Override
    public void complete() {
        session.execute(batchStatement);
    }

    @Override
    public ListenableFuture<Void> completeAsync() {
        return Futures.transform(session.executeAsync(batchStatement), (Function<ResultSet, Void>) input -> null, executor);
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public BatchStatement getBatchStatement() {
        return batchStatement;
    }
}
