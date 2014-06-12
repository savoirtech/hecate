/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.savoirtech.hecate.cql3.util.HecateUtils.*;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface Table {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    double bloom_filter_fp_chance() default UNSPECIFIED_CHANCE;

    String caching() default "";

    String clustering_order() default EMPTY;

    String compaction() default EMPTY;

    String compression() default EMPTY;

    double dclocal_read_repair_chance() default UNSPECIFIED_CHANCE;

    long gc_grace_seconds() default UNSPECIFIED_TIME;

    String name() default EMPTY;

    boolean populate_io_cache_on_flush() default false;

    double read_repair_chance() default UNSPECIFIED_CHANCE;

    boolean replicate_on_write() default true;

    int ttl() default 0;
}
