module FMapper

open System
open FSharp.Reflection
open System.Data.Common
open System.Collections.Generic

/// Strategy to adapt columns
type ColumnStrategy =
    | AsIs
    | Custom of (string -> string)
    | Prefix of string
    | StartIndex of int

let getBoxed<'t> (getter : DbDataReader -> int -> 't) (nullable : bool) (reader : DbDataReader) idx =
    if nullable then
        if reader.IsDBNull idx then None
        else getter reader idx |> Some
        |> box
    else
        getter reader idx |> box

let baseTypes =
    dict [
        typeof<sbyte>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetByte idx)
        typeof<int16>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetInt16 idx)
        typeof<int>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetInt32 idx)
        typeof<int64>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetInt64 idx)
        typeof<single>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetFloat idx)
        typeof<double>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetDouble idx)
        typeof<decimal>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetDecimal idx)
        typeof<bool>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetBoolean idx)
        typeof<char>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetChar idx)
        typeof<DateTime>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetDateTime idx)
        typeof<string>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetString idx)
        typeof<Guid>, getBoxed (fun (reader : DbDataReader) idx -> reader.GetGuid idx)
    ]

let primitiveTypes =
    HashSet(baseTypes |> Seq.map(fun x -> x.Key))

/// Type adapter for a single column
type TypeAdapter<'t> = DataReaderWrapper -> int -> 't
and TypeAdapterBoxed = DataReaderWrapper -> int -> obj
and CustomTypeMap() =
    let map = Dictionary<Type, TypeAdapterBoxed>()

    member self.Count = map.Count

    member self.Add<'t> (f : TypeAdapter<'t>) =
        map.Add(typeof<'t>, fun wrapper i -> f wrapper i |> box)
        self
    
    member self.TryGet(t : Type) =
        match map.TryGetValue(t) with
        | false, _ -> None
        | true, x -> Some x

    static member Empty = new CustomTypeMap()

and CustomTypeAdapter<'t> = DataReaderWrapper -> 't
and CustomTypeAdapterBoxed = DataReaderWrapper -> obj

and CustomTypeAdapterMap() =
    let map = Dictionary<string, CustomTypeAdapterBoxed>()

    member self.Count = map.Count

    member self.Add<'t> (name, f : CustomTypeAdapter<'t>) =
        map.Add(name, f >> box)
        self

    member self.TryGet(name) =
        match map.TryGetValue(name) with
        | false, _ -> None
        | true, x -> Some x

    static member Empty = new CustomTypeAdapterMap()

and DataReaderWrapper(reader : DbDataReader, customTypeMap : CustomTypeMap, customTypeAdapterMap : CustomTypeAdapterMap, columnStrategy : ColumnStrategy) as self =
    let map = Dictionary<string, int * bool>()
    let schema = reader.GetColumnSchemaAsync() |> Async.AwaitTask |> Async.RunSynchronously

    do for c in schema do 
        let ordinal = c.ColumnOrdinal |> Option.ofNullable |> Option.defaultValue -1
        let allowDBNull = c.AllowDBNull |> Option.ofNullable |> Option.defaultValue true

        map.Add(c.ColumnName, (ordinal, allowDBNull))

    let index (name : string) = self.[name] |> fst

    member self.ColumnCount = map.Count

    /// Return a column's index and nullability or None
    member self.TryFind(name : string) =
        match map.TryGetValue name with
        | false, _ -> None
        | true, x -> Some x

    member self.TryFind(idx : int) =
        if idx >= schema.Count then None 
        else Some (idx, schema.[idx].AllowDBNull.GetValueOrDefault(true))
    
    /// Return a column's index and nullability or raise exception
    member self.Item(name : string) =
        match map.TryGetValue name with
        | false, _ -> raise (KeyNotFoundException($"Column not found: '{name}'"))
        | true, x -> x

    member self.Find(colName : string, name) =
        match self.TryFind colName with
        | None -> 
            failwithf "The data source does not contain a field named '%s' (attribute '%s')" colName name
        | Some (idx, nullable) ->
            (idx, nullable)

    member self.Reader = reader

    // raw data getters
    member self.GetBoolean(name : string) = self.Reader.GetBoolean(index name)
    member self.GetByte(name : string) = self.Reader.GetByte(index name)
    member self.GetChar(name : string) = self.Reader.GetChar(index name)
    member self.GetDateTime(name : string) = self.Reader.GetDateTime(index name)
    member self.GetDecimal(name : string) = self.Reader.GetDecimal(index name)
    member self.GetDouble(name : string) = self.Reader.GetDouble(index name)
    member self.GetFloat(name : string) = self.Reader.GetFloat(index name)
    member self.GetGuid(name : string) = self.Reader.GetGuid(index name)
    member self.GetInt16(name : string) = self.Reader.GetInt16(index name)
    member self.GetInt32(name : string) = self.Reader.GetInt32(index name)
    member self.GetInt64(name : string) = self.Reader.GetInt64(index name)
    member self.GetString(name : string) = self.Reader.GetString(index name)

    member self.GetBooleanOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetBoolean(i) |> Some
    member self.GetByteOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetByte(i) |> Some
    member self.GetCharOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetChar(i) |> Some
    member self.GetDateTimeOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetDateTime(i) |> Some
    member self.GetDecimalOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetDecimal(i) |> Some
    member self.GetDoubleOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetDouble(i) |> Some
    member self.GetFloatOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetFloat(i) |> Some
    member self.GetGuidOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetGuid(i) |> Some
    member self.GetInt16Option(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetInt16(i) |> Some
    member self.GetInt32Option(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetInt32(i) |> Some
    member self.GetInt64Option(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetInt64(i) |> Some
    member self.GetStringOption(name : string) = let i = index name in if self.Reader.IsDBNull i then None else self.Reader.GetString(i) |> Some
    
    // data getter with type checking and recursive behaviour
    member self.ValueGetter(idx : int, nullable : bool, propType : Type, name : string) =
        match customTypeMap.TryGet propType with
        | Some tm ->
            fun () ->
                tm self idx
        | None ->
            let t = self.Reader.GetFieldType(idx)
            let getter =
                match baseTypes.TryGetValue(t) with
                | false, _ -> failwithf "Unsupported data type for field '%s': '%s'" name t.Name
                | true, f -> f

            let propType, option =
                if propType.IsGenericType && propType.GetGenericTypeDefinition() = typedefof<option<_>> then
                    propType.GenericTypeArguments.[0], true
                else
                    // this would not work on postgres and possibly other database because
                    // the nullability information of a query result is unknown
                    //if nullable then failwithf "Type mismatch on field '%s': it shouldn't be nullable" name
                    propType, false

            if propType.IsAssignableFrom(t) |> not then 
                failwithf "Type mismatch on field '%s': expected '%s', found '%s'" name propType.Name t.Name
            else
                fun () -> 
                    getter option self.Reader idx

    member self.ValueGetter (name : string, propType : Type) =
        match customTypeAdapterMap.TryGet name with
        | Some custom -> fun () -> custom self
        | None ->
            match columnStrategy with
            | Custom f ->
                let colName = f name
                let idx, nullable = self.Find(colName, name)
                self.ValueGetter(idx, nullable, propType, colName)
            | Prefix x -> 
                let colName = x + name
                let idx, nullable = self.Find(colName, name)
                self.ValueGetter(idx, nullable, propType, colName)
            | AsIs ->
                let idx, nullable = self.Find(name, name)
                self.ValueGetter(idx, nullable, propType, name)
            | StartIndex i -> failwithf "invalid strategy in this context: %A" columnStrategy
            
    member self.ValueGetter (idx : int, propType : Type) =
        let idx = 
            match columnStrategy with
            | StartIndex i -> idx + i
            | _ -> idx

        let name = string idx

        match customTypeAdapterMap.TryGet name with
        | Some custom -> fun () -> custom self
        | None ->
            match self.TryFind idx with
            | None ->
                failwithf "The data source has only %d columns, index %d was requested" self.ColumnCount idx
            | Some (idx, nullable) ->
                self.ValueGetter(idx, nullable, propType, name)
            
module CustomTypeMap =
    let add<'t> (f : TypeAdapter<'t>) (tm : CustomTypeMap) = tm.Add<'t>(f)

module CustomTypeAdapterMap =
    let add<'t> name (f : CustomTypeAdapter<'t>) (tm : CustomTypeAdapterMap) = tm.Add(name, f)
    
let adaptRecord<'t> (customTypeMap : CustomTypeMap) (customAdapter : CustomTypeAdapterMap) (reader : DbDataReader) colStrategy =
    let wrapper = DataReaderWrapper(reader, customTypeMap, customAdapter, colStrategy)
    let props = FSharpType.GetRecordFields(typeof<'t>)

    let getters =
        [|
            for prop in props do
                wrapper.ValueGetter(prop.Name, prop.PropertyType)
        |]

    let nGetters = getters.Length
    let limit = nGetters - 1

    let builder = FSharpValue.PreComputeRecordConstructor(typeof<'t>)

    let buf = Array.zeroCreate<obj> nGetters

    fun () ->
        for i = 0 to limit do
            buf.[i] <- getters.[i] ()
        builder buf |> unbox<'t>

let adaptTuple<'t> (customTypeMap : CustomTypeMap) (customAdapter : CustomTypeAdapterMap) (reader : DbDataReader) colStrategy =
    let wrapper = DataReaderWrapper(reader, customTypeMap, customAdapter, colStrategy)
    let types = FSharpType.GetTupleElements(typeof<'t>)

    let getters =
        [|
            for i, t in Array.indexed types do
                wrapper.ValueGetter(i, t)
        |]

    let nGetters = getters.Length
    let limit = nGetters - 1

    let builder = FSharpValue.PreComputeTupleConstructor(typeof<'t>)

    let buf = Array.zeroCreate<obj> nGetters

    fun () ->
        for i = 0 to limit do
            buf.[i] <- getters.[i] ()
        builder buf |> unbox<'t>

let adaptPrimitive<'t> (reader : DbDataReader) colStrategy =
    let wrapper = DataReaderWrapper(reader, CustomTypeMap.Empty, CustomTypeAdapterMap.Empty, colStrategy)

    let g = wrapper.ValueGetter(0, typeof<'t>)

    fun () ->
        g () |> unbox<'t>

let getAdapter customTypeMap customAdapter reader colStrategy =
    if FSharpType.IsRecord(typeof<'t>) then adaptRecord<'t> customTypeMap customAdapter reader colStrategy
    elif FSharpType.IsTuple(typeof<'t>) then adaptTuple<'t> customTypeMap customAdapter reader colStrategy
    elif typeof<'t> = typeof<obj> then failwithf "Unable to adapt type '%s' - maybe you forgot to specify the mapped type, e.g. RelMapper.Query<MyType>(...)?" typeof<'t>.Name
    elif primitiveTypes.Contains typeof<'t> then adaptPrimitive<'t> reader colStrategy
    else failwithf "Unable to adapt type '%s'" typeof<'t>.Name
        
type ParameterList = (string * obj) list
module ParameterList =
    let empty : ParameterList = []
    let add<'t> k (v : 't) l : ParameterList = 
        (k, box v) :: l

    let ofRecord<'t> (pars : 't) : ParameterList =
        [
            for p in FSharpType.GetRecordFields typeof<'t> do
                p.Name, FSharpValue.GetRecordField(pars, p)
        ]
        |> List.rev

type RelMapper() =
    static member Adapter<'t>(customTypeMap, customAdapter, reader : DbDataReader, ?colStrategy) =
        // if there are no rows return a dummy function: it will be never called.
        // this is needed by sqlite because of type affinity: when the returned dataset is empty
        // there is no reliable way to retrieve correct types. 
        // WARNING: This is a quick and dirty trick, if it causes unexpected 
        //          issues we should find a better way
        if not reader.HasRows then 
            fun () -> Unchecked.defaultof<'t>
        else
            let colStrategy = defaultArg colStrategy AsIs

            getAdapter customTypeMap customAdapter reader colStrategy
        
    static member Adapters<'t1, 't2>(customTypeMap, customAdapter, reader : DbDataReader, col1Strategy, col2Strategy) =
        let a1 = RelMapper.Adapter<'t1>(customTypeMap, customAdapter, reader, col1Strategy)
        let a2 = RelMapper.Adapter<'t2>(customTypeMap, customAdapter, reader, col2Strategy)
        a1, a2

    static member Adapters<'t1, 't2, 't3>(customTypeMap, customAdapter, reader : DbDataReader, col1Strategy, col2Strategy, col3Strategy) =
        let a1 = RelMapper.Adapter<'t1>(customTypeMap, customAdapter, reader, col1Strategy)
        let a2 = RelMapper.Adapter<'t2>(customTypeMap, customAdapter, reader, col2Strategy)
        let a3 = RelMapper.Adapter<'t3>(customTypeMap, customAdapter, reader, col3Strategy)
        a1, a2, a3

    static member Adapters<'t1, 't2, 't3, 't4>(customTypeMap, customAdapter, reader : DbDataReader, col1Strategy, col2Strategy, col3Strategy, col4Strategy) =
        let a1 = RelMapper.Adapter<'t1>(customTypeMap, customAdapter, reader, col1Strategy)
        let a2 = RelMapper.Adapter<'t2>(customTypeMap, customAdapter, reader, col2Strategy)
        let a3 = RelMapper.Adapter<'t3>(customTypeMap, customAdapter, reader, col3Strategy)
        let a4 = RelMapper.Adapter<'t4>(customTypeMap, customAdapter, reader, col4Strategy)
        a1, a2, a3, a4

    static member Adapters<'t1, 't2, 't3, 't4, 't5>(customTypeMap, customAdapter, reader : DbDataReader, col1Strategy, col2Strategy, col3Strategy, col4Strategy, col5Strategy) =
        let a1 = RelMapper.Adapter<'t1>(customTypeMap, customAdapter, reader, col1Strategy)
        let a2 = RelMapper.Adapter<'t2>(customTypeMap, customAdapter, reader, col2Strategy)
        let a3 = RelMapper.Adapter<'t3>(customTypeMap, customAdapter, reader, col3Strategy)
        let a4 = RelMapper.Adapter<'t4>(customTypeMap, customAdapter, reader, col4Strategy)
        let a5 = RelMapper.Adapter<'t5>(customTypeMap, customAdapter, reader, col5Strategy)
        a1, a2, a3, a4, a5
        
    static member GetCommand(conn : DbConnection, query : string, pars : ParameterList option, txn : DbTransaction option) =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- query
        for k, v in defaultArg pars List.empty |> List.rev do
            let par = cmd.CreateParameter()
            par.ParameterName <- k            

            // deal with option<_>
            if isNull v then 
                par.Value <- DBNull.Value
            else
                let t = v.GetType()
                if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>> then
                    par.Value <- t.GetProperty("Value").GetValue(v)
                else
                    par.Value <- v
            
            cmd.Parameters.Add(par) |> ignore

        match txn with
        | None -> ()
        | Some txn ->
            cmd.Transaction <- txn

        cmd

    static member ExecuteRawReader(conn : DbConnection, query : string, pars : ParameterList option, txn : DbTransaction option) =
        let cmd = RelMapper.GetCommand(conn, query, pars, txn)
        let reader = cmd.ExecuteReader()

        let disposer = {new IDisposable with member __.Dispose() = reader.Dispose(); cmd.Dispose()}

        disposer, reader

    static member Query<'t>(conn : DbConnection, query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters) =
        let typeMap = defaultArg typeMap CustomTypeMap.Empty
        let customAdapters = defaultArg customAdapters CustomTypeAdapterMap.Empty

        seq {
            let disposer, reader = RelMapper.ExecuteRawReader(conn, query, pars, txn)
            use _ = disposer 

            let adapter = RelMapper.Adapter<'t>(typeMap, customAdapters, reader)

            while reader.Read() do
                yield adapter ()
        }

    static member Query<'t1, 't2>(conn : DbConnection, query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy) =
        let typeMap = defaultArg typeMap CustomTypeMap.Empty
        let customAdapters = defaultArg customAdapters CustomTypeAdapterMap.Empty

        seq {
            let disposer, reader = RelMapper.ExecuteRawReader(conn, query, pars, txn)
            use _ = disposer 

            let a1, a2 = RelMapper.Adapters<'t1, 't2>(typeMap, customAdapters, reader, defaultArg col1Strategy AsIs, defaultArg col2Strategy AsIs)
    
            while reader.Read() do
                yield a1 (), a2 ()
        }

    static member Query<'t1, 't2, 't3>(conn : DbConnection, query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy) =
        let typeMap = defaultArg typeMap CustomTypeMap.Empty
        let customAdapters = defaultArg customAdapters CustomTypeAdapterMap.Empty

        seq {
            let disposer, reader = RelMapper.ExecuteRawReader(conn, query, pars, txn)
            use _ = disposer 

            let a1, a2, a3 = RelMapper.Adapters<'t1, 't2, 't3>(typeMap, customAdapters, reader, defaultArg col1Strategy AsIs, defaultArg col2Strategy AsIs, defaultArg col3Strategy AsIs)
    
            while reader.Read() do
                yield a1 (), a2 (), a3 ()
        }

    static member Query<'t1, 't2, 't3, 't4>(conn : DbConnection, query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy) =
        let typeMap = defaultArg typeMap CustomTypeMap.Empty
        let customAdapters = defaultArg customAdapters CustomTypeAdapterMap.Empty

        seq {
            let disposer, reader = RelMapper.ExecuteRawReader(conn, query, pars, txn)
            use _ = disposer 

            let a1, a2, a3, a4 = 
                RelMapper.Adapters<'t1, 't2, 't3, 't4>(
                    typeMap, customAdapters, reader, defaultArg col1Strategy AsIs, defaultArg col2Strategy AsIs, defaultArg col3Strategy AsIs, defaultArg col4Strategy AsIs)
    
            while reader.Read() do
                yield a1 (), a2 (), a3 (), a4 ()
        }

    static member Query<'t1, 't2, 't3, 't4, 't5>(conn : DbConnection, query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy, ?col5Strategy) =
        let typeMap = defaultArg typeMap CustomTypeMap.Empty
        let customAdapters = defaultArg customAdapters CustomTypeAdapterMap.Empty

        seq {
            let disposer, reader = RelMapper.ExecuteRawReader(conn, query, pars, txn)
            use _ = disposer 

            let a1, a2, a3, a4, a5 = 
                RelMapper.Adapters<'t1, 't2, 't3, 't4, 't5>(
                    typeMap, customAdapters, reader, defaultArg col1Strategy AsIs, defaultArg col2Strategy AsIs, defaultArg col3Strategy AsIs, defaultArg col4Strategy AsIs, defaultArg col5Strategy AsIs)
    
            while reader.Read() do
                yield a1 (), a2 (), a3 (), a4 (), a5 ()
        }
    
    static member Query<'t, 'p> (conn : DbConnection, query : string, pars : 'p, ?txn, ?typeMap, ?customAdapters) =
        let pars' =
            if FSharpType.IsRecord typeof<'p> then
                ParameterList.ofRecord<'p> pars
            else
                failwithf "pars should be a record or a ParameterList"
        RelMapper.Query<'t>(conn, query, pars', ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)

    static member Query<'t1, 't2, 'p> (conn : DbConnection, query : string, pars : 'p, ?txn, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy) =
        let pars' =
            if FSharpType.IsRecord typeof<'p> then
                ParameterList.ofRecord<'p> pars
            else
                failwithf "pars should be a record or a ParameterList"
        RelMapper.Query<'t1, 't2>(conn, query, pars', ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy)

    static member Query<'t1, 't2, 't3, 'p> (conn : DbConnection, query : string, pars : 'p, ?txn, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy) =
        let pars' =
            if FSharpType.IsRecord typeof<'p> then
                ParameterList.ofRecord<'p> pars
            else
                failwithf "pars should be a record or a ParameterList"
        RelMapper.Query<'t1, 't2, 't3>(conn, query, pars', ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy)

    static member Query<'t1, 't2, 't3, 't4, 'p> (conn : DbConnection, query : string, pars : 'p, ?txn, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy) =
        let pars' =
            if FSharpType.IsRecord typeof<'p> then
                ParameterList.ofRecord<'p> pars
            else
                failwithf "pars should be a record or a ParameterList"
        RelMapper.Query<'t1, 't2, 't3, 't4>(conn, query, pars', ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy, ?col4Strategy=col4Strategy)

    static member Query<'t1, 't2, 't3, 't4, 't5, 'p> (conn : DbConnection, query : string, pars : 'p, ?txn, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy, ?col5Strategy) =
        let pars' =
            if FSharpType.IsRecord typeof<'p> then
                ParameterList.ofRecord<'p> pars
            else
                failwithf "pars should be a record or a ParameterList"
        RelMapper.Query<'t1, 't2, 't3, 't4, 't5>(conn, query, pars', ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy, ?col4Strategy=col4Strategy, ?col5Strategy=col5Strategy)
    
    static member QueryOne<'t>(conn, query, ?pars, ?txn, ?typeMap, ?customAdapters, ?exactlyOne) =
        let exactlyOne = defaultArg exactlyOne false
        let res = RelMapper.Query<'t>(conn, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)
        if exactlyOne then res |> Seq.tryExactlyOne
        else res |> Seq.tryHead

    static member QueryOne<'t, 'p>(conn, query, pars : 'p, ?txn, ?typeMap, ?customAdapters, ?exactlyOne) =
        let exactlyOne = defaultArg exactlyOne false
        let res = RelMapper.Query<'t, 'p>(conn, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)
        if exactlyOne then res |> Seq.tryExactlyOne
        else res |> Seq.tryHead

    static member Execute(conn, query, ?pars, ?txn) =
        use cmd = RelMapper.GetCommand(conn, query, pars, txn)
        cmd.ExecuteNonQuery()

type DbConnection with
    member self.Query<'t>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters) =
        RelMapper.Query<'t>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)

    member self.Query<'t1, 't2>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy) =
        RelMapper.Query<'t1, 't2>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy)

    member self.Query<'t1, 't2, 't3>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy) =
        RelMapper.Query<'t1, 't2, 't3>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy)

    member self.Query<'t1, 't2, 't3, 't4>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy) =
        RelMapper.Query<'t1, 't2, 't3, 't4>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy, ?col4Strategy=col4Strategy)

    member self.Query<'t1, 't2, 't3, 't4, 't5>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy, ?col5Strategy) =
        RelMapper.Query<'t1, 't2, 't3, 't4, 't5>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy, ?col4Strategy=col4Strategy, ?col5Strategy=col5Strategy)
    
    member self.Query<'t, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters) =
        RelMapper.Query<'t, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)

    member self.Query<'t1, 't2, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy) =
        RelMapper.Query<'t1, 't2, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy)

    member self.Query<'t1, 't2, 't3, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy) =
        RelMapper.Query<'t1, 't2, 't3, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy)

    member self.Query<'t1, 't2, 't3, 't4, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy) =
        RelMapper.Query<'t1, 't2, 't3, 't4, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy, ?col4Strategy=col4Strategy)

    member self.Query<'t1, 't2, 't3, 't4, 't5, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?col1Strategy, ?col2Strategy, ?col3Strategy, ?col4Strategy, ?col5Strategy) =
        RelMapper.Query<'t1, 't2, 't3, 't4, 't5, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?col1Strategy=col1Strategy, ?col2Strategy=col2Strategy, ?col3Strategy=col3Strategy, ?col4Strategy=col4Strategy, ?col5Strategy=col5Strategy)
    
    member self.QueryOne<'t>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?exactlyOne) =
        RelMapper.QueryOne<'t>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?exactlyOne=exactlyOne)

    member self.QueryOne<'t, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?exactlyOne) =
        RelMapper.QueryOne<'t, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?exactlyOne=exactlyOne)

    member self.Execute(query, ?pars, ?txn) =
        use cmd = RelMapper.GetCommand(self, query, pars, txn)
        cmd.ExecuteNonQuery()
