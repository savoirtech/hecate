# Hecate POJO Mapping

Hecate allows you to perform CRUD operations in Cassandra using Plain 'Ole Java Objects (POJOs)!  

## PojoDao

### Saving

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = new Person();
person.setFirstName("");
person.setLastName("");
dao.save(person);
```
