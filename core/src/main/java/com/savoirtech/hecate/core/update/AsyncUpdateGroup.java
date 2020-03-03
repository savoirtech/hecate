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
import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.savoirtech.hecate.core.exception.HecateException;
import java.util.stream.Collectors;

public class AsyncUpdateGroup implements UpdateGroup {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final CqlSession session;
    private final List<CompletionStage> futures = Collections.synchronizedList(Lists.newLinkedList());

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public AsyncUpdateGroup(CqlSession session) {
        this.session = session;
    }

//----------------------------------------------------------------------------------------------------------------------
// UpdateGroup Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void addUpdate(Statement statement) {
        futures.add(session.executeAsync(statement));
    }

    @Override
    public void complete() {
        try {
            Uninterruptibles.getUninterruptibly(CompletableFuture.allOf(futures.stream().map(stage -> stage.toCompletableFuture()).collect(Collectors.toList()).toArray(new CompletableFuture[0])));
        } catch (ExecutionException e) {
            throw new HecateException(e, "An unknown exception occurred while executing query.");
        }
    }

    @Override
    public CompletableFuture<Void> completeAsync() {
        return CompletableFuture.allOf(futures.stream()
                .map(stage -> stage.toCompletableFuture())
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]));
    }
}
