# Hecate POJO Mapping

Hecate allows you to perform CRUD operations in Cassandra using Plain 'Ole Java Objects (POJOs)!  

## PojoDao

### Saving Objects

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = new Person();
person.setSsn("123456789");
person.setFirstName("");
person.setLastName("");
dao.save(person);
```

### Deleting Objects

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
dao.delete("123456789");
```
