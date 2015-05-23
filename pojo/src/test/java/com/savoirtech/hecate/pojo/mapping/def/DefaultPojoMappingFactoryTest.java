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

package com.savoirtech.hecate.pojo.mapping.def;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Column;
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.facet.FacetMapping;
import com.savoirtech.hecate.test.AbstractTestCase;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultPojoMappingFactoryTest extends AbstractTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final DefaultPojoMappingFactory factory = new DefaultPojoMappingFactory();

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testCompositeKey() {
        PojoMapping<CensusData> mapping = createMapping(CensusData.class);
        assertColumnNames(mapping.getIdMappings(), "id_country_code", "id_state", "id_zip");
    }

    private <P> PojoMapping<P> createMapping(Class<P> pojoClass) {
        PojoMapping<P> mapping = factory.createPojoMapping(pojoClass);
        return mapping;
    }

    private void assertColumnNames(List<? extends FacetMapping> mappings, String... names) {
        assertEquals(Arrays.asList(names), mappings.stream().map(mapping -> mapping.getFacet().getColumnName()).collect(Collectors.toList()));
    }

    @Test
    public void testSimpleKey() {
        PojoMapping<Person> mapping = createMapping(Person.class);
        assertColumnNames(mapping.getIdMappings(), "id");
        assertColumnNames(mapping.getSimpleMappings(), "first_name", "last_name", "ssn");
    }

    @Test(expected = UncheckedExecutionException.class)
    public void testWithNoConverter() {
        createMapping(NoConverter.class);
    }

    @Test(expected = UncheckedExecutionException.class)
    public void testWithNoConverterFoundOnIdType() {
        createMapping(NoConverter.class);
    }

    @Test(expected = UncheckedExecutionException.class)
    public void testWithNonKeyField() {
        createMapping(WithNonKey.class);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class CensusData {
        @Id
        private PostalCode id;

        private int population;
    }

    public static class NoConverter {
        @Id
        private NoConverterKey key;
    }

    public static class NoConverterKey {
        @PartitionKey
        private Pair<String,String> pair1;

        @ClusteringColumn
        private Pair<String,String> pair2;
    }

    private static class NoId {
        private String property1;
        private String property2;
    }

    public static class NonKeyField {
        private String key;
    }

    public static class Person {
        @Id
        private String id;
        @Column("ssn")
        private String socialSecurityNumber;
        private String firstName;
        private String lastName;
    }

    private static class PostalCode {
        @PartitionKey(order = 0)
        private String countryCode;

        @ClusteringColumn(order = 1,descending = true)
        private String zip;

        @PartitionKey(order = 1)
        private String state;
    }

    public static class WithNonKey {
        @Id
        private NonKeyField key;
    }
}