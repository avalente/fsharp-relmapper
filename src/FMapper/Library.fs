module FMapper

open System
open FSharp.Reflection
open System.Data.Common
open System.Collections.Generic

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

and DataReaderWrapper(reader : DbDataReader, customTypeMap : CustomTypeMap, customTypeAdapterMap : CustomTypeAdapterMap) as self =
    let map = Dictionary<string, int * bool>()
    let schema = reader.GetColumnSchemaAsync() |> Async.AwaitTask |> Async.RunSynchronously

    do for c in schema do map.Add(c.ColumnName, (c.ColumnOrdinal.Value, c.AllowDBNull.Value))

    let index (name : string) = self.[name] |> fst

    member self.ColumnCount = map.Count

    /// Return a column's index and nullability or None
    member self.TryFind(name : string) =
        match map.TryGetValue name with
        | false, _ -> None
        | true, x -> Some x

    member self.TryFind(idx : int) =
        if idx >= schema.Count then None 
        else Some (idx, schema.[idx].AllowDBNull.Value)
    
    /// Return a column's index and nullability or raise exception
    member self.Item(name : string) =
        match map.TryGetValue name with
        | false, _ -> raise (KeyNotFoundException($"Column not found: '{name}'"))
        | true, x -> x

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
                | false, _ -> failwithf "Unsupported data type: '%s'" t.Name
                | true, f -> f

            let propType, option =
                if propType.IsGenericType && propType.GetGenericTypeDefinition() = typedefof<option<_>> then
                    propType.GenericTypeArguments.[0], true
                else
                    if nullable then failwithf "Type mismatch on field '%s': it shouldn't be nullable" name
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
            match self.TryFind name with
            | None -> 
                failwithf "The data source does not contain a field named '%s'" name
            | Some (idx, nullable) ->
                self.ValueGetter(idx, nullable, propType, name)

    member self.ValueGetter (idx : int, propType : Type) =
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
    
let adaptRecord<'t> (customTypeMap : CustomTypeMap) (customAdapter : CustomTypeAdapterMap) (reader : DbDataReader) =
    let wrapper = DataReaderWrapper(reader, customTypeMap, customAdapter)
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

let adaptTuple<'t> (customTypeMap : CustomTypeMap) (customAdapter : CustomTypeAdapterMap) (reader : DbDataReader) =
    let wrapper = DataReaderWrapper(reader, customTypeMap, customAdapter)
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
    static member Adapt<'t>(customTypeMap, customAdapter, reader : DbDataReader) =
        if FSharpType.IsRecord(typeof<'t>) then adaptRecord<'t> customTypeMap customAdapter reader
        elif FSharpType.IsTuple(typeof<'t>) then adaptTuple<'t> customTypeMap customAdapter reader
        elif typeof<'t> = typeof<obj> then failwithf "Unable to adapt type '%s' - maybe you forgot to specify the mapped type, e.g. RelMapper.Query<MyType>(...)?" typeof<'t>.Name
        else failwithf "Unable to adapt type '%s'" typeof<'t>.Name
        
    static member Query<'t>(conn : DbConnection, query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters) =
        let typeMap = defaultArg typeMap CustomTypeMap.Empty
        let customAdapters = defaultArg customAdapters CustomTypeAdapterMap.Empty

        seq {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- query
            for k, v in defaultArg pars List.empty |> List.rev do
                let par = cmd.CreateParameter()
                par.ParameterName <- k
                par.Value <- v
                cmd.Parameters.Add(par) |> ignore

            match txn with
            | None -> ()
            | Some txn ->
                cmd.Transaction <- txn

            use reader = cmd.ExecuteReader()

            let adapter = RelMapper.Adapt<'t>(typeMap, customAdapters, reader)

            while reader.Read() do
                yield adapter ()
        }

    static member Query<'t, 'p> (conn : DbConnection, query : string, pars : 'p, ?txn, ?typeMap, ?customAdapters) =
        let pars' =
            if FSharpType.IsRecord typeof<'p> then
                ParameterList.ofRecord<'p> pars
            else
                failwithf "pars should be a record or a ParameterList"
        RelMapper.Query<'t>(conn, query, pars', ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)

    static member QueryOne<'t>(conn, query, ?pars, ?txn, ?typeMap, ?customAdapters, ?exactlyOne) =
        let exactlyOne = defaultArg exactlyOne false
        let res = RelMapper.Query<'t>(conn, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)
        if exactlyOne then res |> Seq.tryExactlyOne
        else res |> Seq.tryHead

    static member QueryOne<'t, 'p>(conn, query, pars, ?txn, ?typeMap, ?customAdapters, ?exactlyOne) =
        let exactlyOne = defaultArg exactlyOne false
        let res = RelMapper.Query<'t, 'p>(conn, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)
        if exactlyOne then res |> Seq.tryExactlyOne
        else res |> Seq.tryHead

type DbConnection with
    member self.Query<'t>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters) =
        RelMapper.Query<'t>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)

    member self.Query<'t, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters) =
        RelMapper.Query<'t, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters)
    

    member self.QueryOne<'t>(query : string, ?pars : ParameterList, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?exactlyOne) =
        RelMapper.QueryOne<'t>(self, query, ?pars=pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?exactlyOne=exactlyOne)

    member self.QueryOne<'t, 'p>(query : string, pars : 'p, ?txn : DbTransaction, ?typeMap, ?customAdapters, ?exactlyOne) =
        RelMapper.QueryOne<'t, 'p>(self, query, pars, ?txn=txn, ?typeMap=typeMap, ?customAdapters=customAdapters, ?exactlyOne=exactlyOne)
    