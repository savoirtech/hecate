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

// Get a single result
Person person = people.one();

// Get results as a list (all in memory)
List<Person> list = people.list();

// Iterate through results
Iterator<Person> iterator = people.iterate();

// Use Java Streams API...
List<String> lastNames = people.stream().map(Person::getLastName).collect(Collectors.toList());

```

# Bootstrapping

Creating a PojoDaoFactory can be as simple as:

```Java
Session session = ...;
PojoDaoFactory factory = new DefaultPojoDaoFactory(session);
```

This will create a PojoDaoFactory with reasonable default settings.  If you would like Hecate to automatically 
create the schema for you, you can use a CreateSchemaVerifier:
 
 ```Java
 Session session = ...;
 PojoMappingVerifier verifier = new CreateSchemaVerifier(session);
 PojoDaoFactory factory = new DefaultPojoDaoFactory(session, verifier);
 ```