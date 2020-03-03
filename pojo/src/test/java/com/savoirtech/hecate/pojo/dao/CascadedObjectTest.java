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

package com.savoirtech.hecate.pojo.dao;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.UUID;

import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import org.junit.Test;

public class CascadedObjectTest extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testRetrieveWhenCascadedObjectRemoved() {
        Department department = new Department();
        Employee manager = new Employee();
        Address address = new Address();
        manager.setAddress(address);
        department.setManager(manager);

        PojoDao<Department> departmentDao = createPojoDao(Department.class);
        departmentDao.save(department);

        PojoDao<Employee> employeeDao = createPojoDao(Employee.class);
        employeeDao.delete(manager);

        assertHecateException(String.format("Employee with key(s) %s not found in table \"employee\".", manager.getId()), () -> departmentDao.findByKey(department.getId()));

    }
    @Test
    public void testInsertCascaded() {
        Department department = new Department();
        Employee manager = new Employee();
        Address address = new Address();
        manager.setAddress(address);
        department.setManager(manager);

        PojoDao<Department> departmentDao = createPojoDao(Department.class);
        departmentDao.save(department);

        Department found = departmentDao.findByKey(department.getId());
        assertNotNull(found.getManager());
        assertNotNull(found.getManager().getAddress());
    }

    @Test
    public void testDeleteCascaded() {

        Department department = new Department();
        Employee manager = new Employee();
        Address address = new Address();
        manager.setAddress(address);
        department.setManager(manager);


        PojoDao<Address> addressDao = createPojoDao(Address.class);
        PojoDao<Employee> employeeDao = createPojoDao(Employee.class);
        PojoDao<Department> departmentDao = createPojoDao(Department.class);
        departmentDao.save(department);

        assertNotNull(departmentDao.findByKey(department.getId()));

        departmentDao.delete(department);

        assertNull(departmentDao.findByKey(department.getId()));
        assertNull(employeeDao.findByKey(manager.getId()));
        assertNull(addressDao.findByKey(address.getId()));
    }

    public static class Address {
        @PartitionKey
        private final String id = UUID.randomUUID().toString();

        public String getId() {
            return id;
        }
    }

    public static class Department {
        @PartitionKey
        private final String id = UUID.randomUUID().toString();

        private Employee manager;

        public String getId() {
            return id;
        }

        public Employee getManager() {
            return manager;
        }

        public void setManager(Employee manager) {
            this.manager = manager;
        }
    }

    public static class Employee {
        @PartitionKey
        private final String id = UUID.randomUUID().toString();

        private Address address;

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        public String getId() {
            return id;
        }
    }
}
