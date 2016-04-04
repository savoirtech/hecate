# Hecate POJO Mapping

Hecate allows you to perform "CRUD" (create, read, update, and delete) operations in Cassandra using Plain 'Ole Java Objects (POJOs)!  

# PojoDao

## A Simple Pojo

For our examples, let's consider a simple "Person" class:

```Java
public class Person {
  @PartitionKey
  private String ssn;
  
  private String lastName;
  private String firstName;
  
  // Getters/setters
}
```

The @PartitionKey annotation identifies a field as a member of the "partition key" of the table which will be used
to store Person objects.  The lastName and firstName fields will automatically be persisted.

## Saving Objects

```Java
PojoDao<Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = new Person();
person.setSsn("123456789");
person.setFirstName("Super");
person.setLastName("Man");
dao.save(person);
```

## Deleting Objects

```Java
PojoDao<Person> dao = pojoDaoFactory.createPojoDao(Person.class);
dao.delete("123456789");
```

## Updating Objects

```Java
PojoDao<Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = ...;

person.setFirstName("Iron");
dao.save(person);
```

## Retrieving Objects

### Find by Key

```Java
PojoDao<Person> dao = pojoDaoFactory.createPojoDao(Person.class);
Person person = dao.findByKey("123456789");
```

### Find By Keys

```Java
PojoDao<Person> dao = pojoDaoFactory.createPojoDao(Person.class);
PojoMultiQuery<Person> query = dao.findByKeys();
QueryResult<Person> result = query.add("12345").add("67890").execute();
```

### Building Queries

```Java
PojoDao<Person> dao = pojoDaoFactory.createPojoDao(Person.class);
PojoQuery<Person> query = dao.find().eq("lastName").build();
QueryResult<Person> smiths = query.execute("Smith"); 
```

### Custom Queries (Bring Your Own Where Clause)

```Java
PojoDao<Person> dao = pojoDaoFactory.createPojoDao(Person.class);
PojoQuery<Person> query = dao.find(where -> where.and(eq("last_name", bindMarker())));
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

// Use Java 8 Streams API...
List<String> lastNames = people.stream().map(Person::getLastName).collect(Collectors.toList());

```

# Bootstrapping

Creating a PojoDaoFactory can be as simple as:

```Java
Session session = ...;
PojoDaoFactory factory = new DefaultPojoDaoFactoryBuilder(session).build();
```

This will create a PojoDaoFactory with reasonable default settings.  If you would like Hecate to automatically 
create the schema for you, you can use a CreateSchemaListener:
 
 ```Java
 Session session = ...;
 PojoDaoFactory factory = new DefaultPojoDaoFactoryBuilder(session)
                                .withListener(new CreateSchemaListener(session))
                                .build();
 ```