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

package com.savoirtech.hecate.core.util;

import java.util.function.BiConsumer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

public class StopWatchLogger {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final StopWatch stopWatch = new StopWatch();
    private final Logger logger;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public StopWatchLogger(Logger logger) {
        this.logger = logger;
        stopWatch.start();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void debug(String message, Object... params) {
        log(logger::debug, message, params);
    }

    private void log(BiConsumer<String,Object> consumer, String message, Object... params) {
        consumer.accept(message + " ({} ms).", ArrayUtils.add(params, stopWatch.getTime()));
        reset();
    }

    public void error(String message, Object... params) {
        log(logger::error, message, params);
    }

    public void info(String message, Object... params) {
        log(logger::info, message, params);
    }

    public void reset() {
        stopWatch.stop();
        stopWatch.reset();
        stopWatch.start();
    }

    public void warn(String message, Object... params) {
        log(logger::warn, message, params);
    }
}
