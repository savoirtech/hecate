# Hecate POJO

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
 
# Composite Keys
 
Using "partition keys" and "clustering columns", you can slice and dice your data in various ways in order to make 
retrieval very efficient.  Hecate's API is designed with composite keys in mind:
 
```Java
public class Address {
 
  @PartitionKey(order=1)
  private String country;
   
  @PartitionKey(order=2)
  private String state;
   
  @ClusteringColumn
  private String postalCode;   
}
 
```
 
Notice the "order" attributes on the @PartitionKey annotations.  We are not guaranteed that the fields will be found
in the same order they are defined in the source code when asking for them using Java reflection, so we use the order 
attribute to sort the @PartitionKeys correctly (can be used on @ClusteringColumn also).

## Finding by a Composite Key

Finding an Address by its full composite key is simple:

```Java
PojoDao<Address> dao = daoFactory.createPojoDao(Address.class);
Address address = dao.findByKey("US", "CO", "80127");
```

Here, we pass in the values for the country, state, and postalCode fields.  The keys must be presented in the correct 
order.

## Finding Objects by Composite Keys

```Java
PojoDao<Address> dao = daoFactory.createPojoDao(Address.class);
PojoMultiQuery<Address> query = dao.findByKeys();
QueryResult<Address> result = query
                               .add("US", "CO", "80127")
                               .add("US", "OH", "45030")
                               .execute();
```