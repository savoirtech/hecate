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

package com.savoirtech.hecate.pojo.binding.def;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Test;

public class DefaultPojoBindingFactoryTest extends AbstractDaoTestCase {
    @Test
    public void testWithOnlyClusteringColumn() {
        assertHecateException("No @PartitionKey facets found for POJO type \"com.savoirtech.hecate.pojo.binding.def.DefaultPojoBindingFactoryTest.OnlyClustering\".", () -> getBindingFactory().createPojoBinding(OnlyClustering.class));
    }


    @Test
    public void testWithNoConverterForSimpleKey() {
        assertHecateException("No converter found for @PartitionKey facet \"id\".", () -> getBindingFactory().createPojoBinding(NoConverter.class));
    }

    @Test
    public void testWithNoKeys() {
        assertHecateException("No key facets found for POJO type \"com.savoirtech.hecate.pojo.binding.def.DefaultPojoBindingFactoryTest.NoKeys\".", () -> getBindingFactory().createPojoBinding(NoKeys.class));
    }

    @Test
    public void testWithNoConverterForMapKey() {
        assertHecateException("Invalid facet \"map\"; no converter registered for key type \"java.io.Serializable\".", () -> getBindingFactory().createPojoBinding(NoConverterMapKey.class));
    }

    @Test
    public void testWithCircularReferences() {
        getBindingFactory().createPojoBinding(Person.class);
    }

    public static class NoConverterMapKey extends UuidEntity {
        private Map<Serializable, String> map;


    }
    public static class NoKeys {

    }

    public static class NoConverter {
        @PartitionKey
        private Serializable id;
    }

    public static class OnlyClustering {
        @ClusteringColumn
        private String cluster;
    }


    public static class Person extends UuidEntity {
        private List<Person> children;
    }
}