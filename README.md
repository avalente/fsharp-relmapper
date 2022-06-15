# FSharp.RelMapper

### What
This is a simple reflection-based relational mapper for F# data structures, heavily inspired by [Dapper](https://github.com/DapperLib/Dapper).

### Why
[Dapper](https://github.com/DapperLib/Dapper) is a very simple and effective way to map SQL query results to objects but being designed for C# classes it doesn't shine when used with native F# data structures; you can still use it on F# records but you will face a number of issues:

1. Option types are not supported out of the box, some additional dependency must be pulled in or a custom adapter must be written.
1. Mapping is performed by position so if you try to map the results of a query like `select ColA, ColB from ...` to a record like `{ ColB : ...; ColA : ...}` you will get an error.
1. Anonymous records are basically unusable: it works only if your query returns the columns in alphabetical order.
1. Error messages aren't very helpful, for instance: `A parameterless default constructor or one matching signature (System.String ColA, System.Int32 ColB) is required for MyModule+MyType materialization`.

### How

Install the package with your favourite tool, for instance:

```sh
dotnet add <MyProject> package FSharp.RelMapper
```

A simple example:

```fsharp
open FSharp.RelMapper
open Microsoft.Data.SqlClient

let connectionString = "Server=localhost;Database=<mydb>;UID=<uid>;PWD=<password>;Encrypt=false"

// table create with the following code:
// create schema my_schema
// GO
// create table my_schema.my_table(id uniqueidentifier primary key, number int not null, description varchar(255) null, value decimal not null)
// go
// insert into my_schema.my_table values
//     ('f74c5e59-e145-430d-aa08-19f67c047863', 1, 'record 1', 1.0), 
//     ('f74c5e59-e145-430d-aa08-19f67c047864', 1, 'record 2', 2.0), 
//     ('f74c5e59-e145-430d-aa08-19f67c047865', 1, null, 3.0), 
//     ('f74c5e59-e145-430d-aa08-19f67c047866', 2, 'record 4', 4.0)    

type MyRecord = 
    {
        Id : Guid
        Category : int 
        Description : string option
        Value : decimal
    }


let getById (id : Guid) (connection : SqlConnection) =
    let query = "select id as Id, number as Category, description as Description, value as Value from my_schema.my_table where id=@id"
    let pars = ParameterList.empty |> ParameterList.add "id" id
    connection.Query<MyRecord>(query, pars)
    |> Seq.tryExactlyOne

use connection = new SqlConnection(connectionString)
connection.Open()
getById (Guid "f74c5e59-e145-430d-aa08-19f67c047863") connection

// output is: 
// Some { Id = f74c5e59-e145-430d-aa08-19f67c047863
//        Category = 1
//        Description = Some "record 1"
//        Value = 1M }

```

Please refer to the "Examples" folder for more use cases.

### Documentation

Currently you can map query results to:

1. F# records including anonymous records and struct
1. F# tuples

Record mapping is performed by name (case sensitive) while tuple mapping is performed by position.

The set of basic data types supported by `DbDataReader` are supported out of the box:

- sbyte
- int16
- int
- int64
- single
- double
- decimal
- bool
- char
- DateTime
- string
- Guid

SQL NULLs are mapped automatically on `Option` types.

The main entry point is the `RelMapper` namespace: you should use its members (mainly `Query`). An extension method `Query` is provided, on the type `DbConnection` so you can use it directly on your `ADO.NET` connection object.

A couple of extension point are provided:

- custom type adapter (`typeMap` argument): you provide a function to build a value of a type from the underlying data; this was designed mainly to build discriminated unions values out of values stored in database tables
- custom field adapter (`customAdapters` argument): you provide a function to build a value for a given field (named for records or stringified index for tuples, e.g. "1" for the second element).

Refer to the notebooks in the "Examples" folder or to the unit test for usage examples.

### Reflection is bad!

True, using reflection makes the code slow and fragile and in F# we have the wonderful tools named `type providers`, but there are cases in which a more "dynamic" approach is needed, in those cases a simple reflection-based tool may be a good compromise.

About performance, some simple benchmarks show that basic mapper speed is compatible with `dapper`'s.

### Unit test

To run unit test with code coverage:

```sh
dotnet test --collect:"XPlat Code Coverage"
```