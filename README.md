# Database-Systems


This repo contains a bare-bones database implementation, which supports
executing simple transactions in series. The start code is cloned from 
berkeley-cs186/sp20-moocbase


### index

The `index` directory contains a skeleton for implementing B+ tree indices.

### memory

The `memory` directory contains classes for managing the loading of data
into and out of memory (in other words, buffer management).

The `BufferFrame` class represents a single buffer frame (page in the buffer
pool) and supports pinning/unpinning and reading/writing to the buffer frame.
All reads and writes require the frame be pinned (which is often done via the
`requireValidFrame` method, which reloads data from disk if necessary, and then
returns a pinned frame for the page).

The `BufferManager` interface is the public interface for the buffer manager of
our DBMS.

The `BufferManagerImpl` class implements a buffer manager using
a write-back buffer cache with configurable eviction policy. It is responsible
for fetching pages (via the disk space manager) into buffer frames, and returns
Page objects to allow for manipulation of data in memory.

The `Page` class represents a single page. When data in the page is accessed or
modified, it delegates reads/writes to the underlying buffer frame containing
the page.

The `EvictionPolicy` interface defines a few methods that determine how the
buffer manager evicts pages from memory when necessary. Implementations of these
include the `LRUEvictionPolicy` (for LRU) and `ClockEvictionPolicy` (for clock).

### io

The `io` directory contains classes for managing data on-disk (in other words,
disk space management).

The `DiskSpaceManager` interface is the public interface for the disk space
manager of our DBMS.

The `DiskSpaceMangerImpl` class is the implementation of the disk space
manager, which maps groups of pages (partitions) to OS-level files, assigns
each page a virtual page number, and loads/writes these pages from/to disk.

### query

The `query` directory contains classes for managing and manipulating queries.

The various operator classes are query operators (pieces of a query).

The `QueryPlan` class represents a plan for executing a query (which we will be
covering in more detail later in the semester). It currently executes the query
as given (runs things in logical order, and performs joins in the order given),
but you will be implementing
a query optimizer to run the query in a more efficient manner.

### recovery

The `recovery` directory contains a skeleton for implementing database recovery
a la ARIES.

### table

The `table` directory contains classes representing entire tables and records.

The `Table` class is, as the name suggests, a table in our database. See the
comments at the top of this class for information on how table data is layed out
on pages.

The `Schema` class represents the _schema_ of a table (a list of column names
and their types).

The `Record` class represents a record of a table (a single row). Records are
made up of multiple DataBoxes (one for each column of the table it belongs to).

The `RecordId` class identifies a single record in a table.

The `HeapFile` interface is the interface for a heap file that the `Table` class
uses to find pages to write data to.

The `PageDirectory` class is an implementation of `HeapFile` that uses a page
directory.

#### table/stats

The `table/stats` directory contains classes for keeping track of statistics of
a table. These are used to compare the costs of different query plans, when 
implementing query optimization.

### Transaction.java

The `Transaction` interface is the _public_ interface of a transaction - it
contains methods that users of the database use to query and manipulate data.

This interface is partially implemented by the `AbstractTransaction` abstract
class, and fully implemented in the `Database.Transaction` inner class.

### TransactionContext.java

The `TransactionContext` interface is the _internal_ interface of a transaction -
it contains methods tied to the current transaction that internal methods
(such as a table record fetch) may utilize.

The current running transaction's transaction context is set at the beginning
of a `Database.Transaction` call (and available through the static
`getCurrentTransaction` method) and unset at the end of the call.

This interface is partially implemented by the `AbstractTransactionContext` abstract
class, and fully implemented in the `Database.TransactionContext` inner class.

### Database.java

The `Database` class represents the entire database. It is the public interface
of our database - we do not parse SQL statements in our database, and instead,
users of our database use it like a Java library.

All work is done in transactions, so to use the database, a user would start
a transaction with `Database#beginTransaction`, then call some of
`Transaction`'s numerous methods to perform selects, inserts, and updates.

For example:
```java
Database db = new Database("database-dir");

try (Transaction t1 = db.beginTransaction()) {
    Schema s = new Schema(
        Arrays.asList("id", "firstName", "lastName"),
        Arrays.asList(Type.intType(), Type.stringType(10), Type.stringType(10))
    );
    t1.createTable(s, "table1");
    t1.insert("table1", Arrays.asList(
        new IntDataBox(1),
        new StringDataBox("John", 10),
        new StringDataBox("Doe", 10)
    ));
    t1.insert("table1", Arrays.asList(
        new IntDataBox(2),
        new StringDataBox("Jane", 10),
        new StringDataBox("Doe", 10)
    ));
    t1.commit();
}

try (Transaction t2 = db.beginTransaction()) {
    // .query("table1") is how you run "SELECT * FROM table1"
    Iterator<Record> iter = t2.query("table1").execute();

    System.out.println(iter.next()); // prints [1, John, Doe]
    System.out.println(iter.next()); // prints [2, Jane, Doe]

    t2.commit();
}

db.close();
```

More complex queries can be found in
[`src/test/java/edu/berkeley/cs186/database/TestDatabase.java`](src/test/java/edu/berkeley/cs186/database/TestDatabase.java).
