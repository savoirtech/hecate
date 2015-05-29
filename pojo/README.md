# Hecate POJO Mapping

Hecate allows you to perform "CRUD" (create, read, update, and delete) operations in Cassandra using Plain 'Ole Java Objects (POJOs)!  

# PojoDao

## Saving Objects

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = new Person();
person.setSsn("123456789");
person.setFirstName("Super");
person.setLastName("Man");
dao.save(person);
```

## Deleting Objects

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
dao.delete("123456789");
```

## Updating Objects

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = ...;

person.setFirstName("Iron");
dao.save(person);
```

## Retrieving Objects

### Find by Id

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = dao.findById("123456789");
```

### Find By Ids

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
QueryResult<Person> people = dao.findByIds(Arrays.asList("123456789", "987654321"));
```

### Custom Queries

```Java
PojoDao<String,Person> dao = pojoDaoFactory.createPojoDao(Person.class);
PojoQuery<Person> query = dao.find().eq("lastName").build();
QueryResult<Person> smiths = query.execute("Smith"); 
```

### QueryResult

```Java
QueryResult<Person> people = ...;

Person person = people.one();

List<Person> list = people.list();

Iterator<Person> iterator = people.iterate();
```