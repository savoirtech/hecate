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

package com.savoirtech.hecate.core.metrics;

import com.codahale.metrics.Timer;

import java.util.function.Supplier;

public class MetricsUtils {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static void doWithTimer(Timer timer, Runnable runnable) {
        Timer.Context context = timer.time();
        try {
            runnable.run();
        } finally {
            context.stop();
        }
    }

    public static <T> T returnWithTimer(Timer timer, Supplier<T> supplier) {
        Timer.Context context = timer.time();
        try {
            return supplier.get();
        } finally {
            context.stop();
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    private MetricsUtils() {

    }
}
