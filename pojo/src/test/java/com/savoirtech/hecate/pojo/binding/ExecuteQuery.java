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

package com.savoirtech.hecate.pojo.binding;

import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Embedded;
import com.savoirtech.hecate.annotation.Key;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.dao.PojoDao;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Test;

public class ExecuteQuery extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testExecuteQuery() {
        PojoDao<Person> personDao = createPojoDao(Person.class);
        PojoDao<Department> departmentDao = createPojoDao(Department.class);
        Department dept = new Department();
        dept.setId("sales");
        dept.setName("Sales");


        Person person = new Person();
        PersonKey key = new PersonKey();
        key.setFirstName("Slappy");
        key.setLastName("White");

        Address address = new Address();
        address.setAddress1("1 Main St.");
        address.setAddress2("apt. B");
        address.setCity("Winchestertonfieldville");
        address.setState("IA");
        address.setZip("11111");
        person.setAddress(address);
        person.setKey(key);
        person.setDob(new Date());

        dept.setEmployees(Lists.newArrayList(person));

        departmentDao.save(dept);


        personDao.deleteByKeys("White", "Slappy");

//        SimplePojo pojo = new SimplePojo();
//        pojo.setId("1");
//        NestedPojo nested = new NestedPojo();
//        nested.setId("2");
//        pojo.setPojoList(Lists.newArrayList(nested));
//        dao.save(pojo);
//        PojoQuery<SimplePojo> query = dao.find().eq("id").build();
//        query.execute("1").stream().forEach(System.out::println);
//        dao.delete(pojo);



//        PojoStatementFactory statementFactory = new DefaultPojoStatementFactory(session);
//
//        session.execute(binding.createTable("my_pojo"));
//
//        TableMetadata tableMetadata = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable("my_pojo");
//        binding.verifySchema(tableMetadata);
//
//        PreparedStatement insert = session.prepare(binding.insertInto("my_pojo"));
//
//        SimplePojo pojo = new SimplePojo();
//        pojo.setId("1");
//        session.execute(binding.bindInsert(insert, pojo));
//
//
//        DefaultPojoQueryContext context = new DefaultPojoQueryContext(5000, session, statementFactory);
//
//        binding.selectFrom("my_pojo").and(eq("id", bindMarker()));
//
//        PojoQuery<SimplePojo> query = new CustomPojoQuery<>(session, binding, context, binding.selectFrom("my_pojo").and(eq("id", bindMarker())));
//        MappedQueryResult<SimplePojo> result = query.execute("1");
//        result.stream().forEach(System.out::println);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class Address {
        private String address1;
        private String address2;
        private String city;
        private String state;
        private String zip;

        public String getAddress1() {
            return address1;
        }

        public void setAddress1(String address1) {
            this.address1 = address1;
        }

        public String getAddress2() {
            return address2;
        }

        public void setAddress2(String address2) {
            this.address2 = address2;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getZip() {
            return zip;
        }

        public void setZip(String zip) {
            this.zip = zip;
        }
    }

    public static class Department {
        @PartitionKey
        private String id;

        private String name;

        private List<Person> employees;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Person> getEmployees() {
            return employees;
        }

        public void setEmployees(List<Person> employees) {
            this.employees = employees;
        }
    }

    public static class Person {
        @Key
        private PersonKey key;

        private Date dob;

        @Embedded
        private Address address;

        public PersonKey getKey() {
            return key;
        }

        public void setKey(PersonKey key) {
            this.key = key;
        }

        public Date getDob() {
            return dob;
        }

        public void setDob(Date dob) {
            this.dob = dob;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }
    }

    public static class PersonKey {
        @PartitionKey
        private String lastName;
        @ClusteringColumn
        private String firstName;

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }
    }
}
