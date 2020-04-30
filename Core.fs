namespace Flux.Neo4j

open System
open Hopac
open Neo4j.Driver
open System.Collections.Generic

module internal ValueOption =

    let ofPair (r, v) =
        if r then ValueSome v else ValueNone

module Driver =

    let create' uri auth conf = GraphDatabase.Driver(uri :> Uri, auth, Action<_> conf)

    let create uri auth = GraphDatabase.Driver(uri :> Uri, auth :> IAuthToken)

module internal SimpleQuery =

    let inline run (query: Query) (queryRunner: IQueryRunner) = query |> queryRunner.Run

    let inline run' (query: Query) conf (session: ISession) = session.Run(query, Action<_> conf)

module internal AsyncQuery =

    let inline runAsync (query: Query) (queryRunner: IAsyncQueryRunner) =
        query
        |> queryRunner.RunAsync
        |> Job.awaitTask

    let inline runAsync' (query: Query) conf (session: IAsyncSession) =
        session.RunAsync(query, Action<_> conf) |> Job.awaitTask

module internal RxQuery =

    let run (query: Query) (queryRunner: IRxRunnable) = query |> queryRunner.Run

    let run' (query: Query) conf (session: IRxSession) = session.Run(query, Action<_> conf)

module Transaction =

    let commit (tr: ITransaction) = tr.Commit()

    let rollback (tr: ITransaction) = tr.Rollback()

    let run query tr = SimpleQuery.run query tr

module AsyncTransaction =

    let commitAsync (tr: IAsyncTransaction) = tr.CommitAsync() |> Job.awaitUnitTask

    let rollbackAsync (tr: IAsyncTransaction) = tr.RollbackAsync() |> Job.awaitUnitTask

    let runAsync query tr = AsyncQuery.runAsync query tr

module RxTransaction =

    let commitRx (tr: IRxTransaction) = tr.Commit()

    let rollbackRx (tr: IRxTransaction) = tr.Rollback()

    let runRx query tr = RxQuery.run query tr

module TransactionConfigBuilder =

    let withMetadata metadata (builder: TransactionConfigBuilder) = builder.WithMetadata metadata

    let withTimeout timeout (builder: TransactionConfigBuilder) = builder.WithTimeout timeout

module Session =

    let create' conf (driver: IDriver) = driver.Session(Action<_> conf)

    let create (driver: IDriver) = driver.Session()

    let beginTransaction (session: ISession) = session.BeginTransaction()

    let beginTransaction' conf (session: ISession) =
        Action<_> conf
        |> session.BeginTransaction

    let readTransaction work (session: ISession) =
        (Func<_, _>(work >> startAsTask))
        |> session.ReadTransaction
        |> Job.awaitTask

    let readTransaction' conf work (session: ISession) =
        session.ReadTransaction(Func<_, _>(work >> startAsTask), Action<_> conf) |> Job.awaitTask

    let writeTransaction work (session: ISession) =
        (Func<_, _>(work >> startAsTask))
        |> session.WriteTransaction
        |> Job.awaitTask

    let writeTransaction' conf work (session: ISession) =
        session.WriteTransaction(Func<_, _>(work >> startAsTask), Action<_> conf) |> Job.awaitTask

    let lastBookmark (session: ISession) = session.LastBookmark

    let run query session = SimpleQuery.run query session

    let run' query conf session = SimpleQuery.run' query conf session

module AsyncSession =

    let create' conf (driver: IDriver) = driver.AsyncSession(Action<_> conf)

    let create (driver: IDriver) = driver.AsyncSession()

    let beginTransactionAsync (session: IAsyncSession) = session.BeginTransactionAsync() |> Job.awaitTask

    let beginTransactionAsync' conf (session: IAsyncSession) =
        Action<_> conf
        |> session.BeginTransactionAsync
        |> Job.awaitTask

    let closeAsync (session: IAsyncSession) = session.CloseAsync() |> Job.awaitUnitTask

    let readTransactionAsync work (session: IAsyncSession) =
        (Func<_, _>(work >> startAsTask))
        |> session.ReadTransactionAsync
        |> Job.awaitTask

    let readTransactionAsync' conf work (session: IAsyncSession) =
        session.ReadTransactionAsync(Func<_, _>(work >> startAsTask), Action<_> conf) |> Job.awaitTask

    let writeTransactionAsync work (session: IAsyncSession) =
        (Func<_, _>(work >> startAsTask))
        |> session.WriteTransactionAsync
        |> Job.awaitTask

    let writeTransactionAsync' conf work (session: IAsyncSession) =
        session.WriteTransactionAsync(Func<_, _>(work >> startAsTask), Action<_> conf) |> Job.awaitTask

    let lastBookmark (session: IAsyncSession) = session.LastBookmark

    let runAsync query session = AsyncQuery.runAsync query session

    let runAsync' query conf session = AsyncQuery.runAsync' query conf session

module RxSession =

    let create' conf (driver: IDriver) = driver.RxSession(Action<_> conf)

    let create (driver: IDriver) = driver.AsyncSession()

    let beginTransactionRx (session: IRxSession) = session.BeginTransaction()

    let beginTransactionRx' conf (session: IRxSession) = Action<_> conf |> session.BeginTransaction

    let closeRx (session: IRxSession) = session.Close()

    let readTransactionRx work (session: IRxSession) = (Func<_, _> work) |> session.ReadTransaction

    let readTransactionRx' conf work (session: IRxSession) = session.ReadTransaction(Func<_, _> work, Action<_> conf)

    let writeTransactionRx work (session: IRxSession) = (Func<_, _> work) |> session.WriteTransaction

    let writeTransactionRx' conf work (session: IRxSession) = session.WriteTransaction(Func<_, _> work, Action<_> conf)

    let lastBookmark (session: IRxSession) = session.LastBookmark

    let run query session = RxQuery.run query session

    let run' query conf session = RxQuery.run' query conf session

module ResultCursor =
    let inline currentRecord (cursor: IResultCursor) = cursor.Current

    let inline fetchAsync (cursor: IResultCursor) = cursor.FetchAsync() |> Job.awaitTask

    let inline keysAsync (cursor: IResultCursor) = cursor.KeysAsync() |> Job.awaitTask

    let inline peekAsync (cursor: IResultCursor) = cursor.PeekAsync() |> Job.awaitTask

    let inline consumeAsync (cursor: IResultCursor) = cursor.ConsumeAsync() |> Job.awaitTask

    let tryNextAsync (cursor: IResultCursor) =
        cursor
        |> fetchAsync
        |> Job.map (fun r ->
            if r then
                cursor
                |> currentRecord
                |> ValueSome
            else
                ValueNone)

    let toSeq (cursor: IResultCursor) =
        let enumerator() =
            { new IEnumerator<IRecord> with
                member __.Reset(): unit = invalidOp "Cannot reset IResultCursor"
                member __.Current: IRecord = currentRecord cursor
                member __.Current: obj = upcast currentRecord cursor
                member __.Dispose(): unit = ()
                member __.MoveNext(): bool =
                    cursor
                    |> fetchAsync
                    |> run }
        { new IEnumerable<IRecord> with
            member __.GetEnumerator(): Collections.IEnumerator = upcast enumerator()
            member __.GetEnumerator(): IEnumerator<IRecord> = enumerator() }

module RxResult =
    let inline keys (result: IRxResult) = result.Keys()

    let inline consume (result: IRxResult) = result.Consume()

    let inline records (result: IRxResult) = result.Records()

type Property =
    | Null
    | List of Property seq
    | Map of IDictionary<string, obj> //wrap this
    | Boolean of bool
    | Integer of int64
    | Float of double
    | String of string
    | ByteArray of byte []
    | Date of LocalDate
    | Time of OffsetTime
    | LocalTime of LocalTime
    | DateTime of ZonedDateTime
    | LocalDateTime of LocalDateTime
    | Duration of Duration
    | Point of Point

module Property =

    let rec ofObj (obj: obj) =
        match obj with
        | x when isNull x -> Null
        | :? (obj seq) as v ->
            v
            |> Seq.map ofObj
            |> List
        | :? IDictionary<string, obj> as v -> Map v
        | :? bool as v -> Boolean v
        | :? int64 as v -> Integer v
        | :? double as v -> Float v
        | :? string as v -> String v
        | :? (byte []) as v -> ByteArray v
        | :? LocalDate as v -> Date v
        | :? OffsetTime as v -> Time v
        | :? LocalTime as v -> LocalTime v
        | :? ZonedDateTime as v -> DateTime v
        | :? LocalDateTime as v -> LocalDateTime v
        | :? Duration as v -> Duration v
        | :? Point as v -> Point v
        | _ ->
            (obj.GetType().FullName)
            |> sprintf "Unknown property type: %s"
            |> failwith

    let tryOfObj (obj: obj) =
        match obj with
        | x when isNull x -> ValueSome(Null)
        | :? (obj seq) as v ->
            v
            |> Seq.map ofObj
            |> List
            |> ValueSome
        | :? IDictionary<string, obj> as v -> ValueSome(Map v)
        | :? bool as v -> ValueSome(Boolean v)
        | :? int64 as v -> ValueSome(Integer v)
        | :? double as v -> ValueSome(Float v)
        | :? string as v -> ValueSome(String v)
        | :? (byte []) as v -> ValueSome(ByteArray v)
        | :? LocalDate as v -> ValueSome(Date v)
        | :? OffsetTime as v -> ValueSome(Time v)
        | :? LocalTime as v -> ValueSome(LocalTime v)
        | :? ZonedDateTime as v -> ValueSome(DateTime v)
        | :? LocalDateTime as v -> ValueSome(LocalDateTime v)
        | :? Duration as v -> ValueSome(Duration v)
        | :? Point as v -> ValueSome(Point v)
        | _ -> ValueNone

    let mapTryFind key (map: IDictionary<_, _>) =
        key
        |> map.TryGetValue
        |> ValueOption.ofPair

module Node =
    let id' (node: INode) = node.Id
    let labels (node: INode) = node.Labels
    let props (node: INode) = node.Properties

    let tryGetProperty key (node: INode) =
        key
        |> node.Properties.TryGetValue
        |> ValueOption.ofPair
        |> ValueOption.map Property.ofObj

    let tryOfObj (obj: obj) =
        match obj with
        | :? INode as node -> ValueSome node
        | _ -> ValueNone

module Relationship =
    let id' (relationship: IRelationship) = relationship.Id
    let type' (relationship: IRelationship) = relationship.Type
    let startNodeId (relationship: IRelationship) = relationship.StartNodeId
    let endNodeId (relationship: IRelationship) = relationship.EndNodeId
    let props (relationship: IRelationship) = relationship.Properties

    let tryGetProperty key (relationship: IRelationship) =
        key
        |> relationship.Properties.TryGetValue
        |> ValueOption.ofPair
        |> ValueOption.map Property.ofObj

    let tryOfObj (obj: obj) =
        match obj with
        | :? IRelationship as relationship -> ValueSome relationship
        | _ -> ValueNone

module Path =
    let startNode (path: IPath) = path.Start
    let endNode (path: IPath) = path.End
    let nodes (path: IPath) = path.Nodes
    let relationships (path: IPath) = path.Relationships

    let tryOfObj (obj: obj) =
        match obj with
        | :? IPath as path -> ValueSome path
        | _ -> ValueNone

type RecordValue =
    | Node of INode
    | Relationship of IRelationship
    | Path of IPath
    | Property of Property

module RecordValue =

    let ofObj raw =
        let c1() =
            raw
            |> Property.tryOfObj
            |> ValueOption.map Property

        let c2() =
            raw
            |> Node.tryOfObj
            |> ValueOption.map Node

        let c3() =
            raw
            |> Relationship.tryOfObj
            |> ValueOption.map Relationship

        let c4() =
            raw
            |> Path.tryOfObj
            |> ValueOption.map Path

        c1()
        |> ValueOption.orElseWith (c2 >> ValueOption.orElseWith (c3 >> ValueOption.orElseWith c4))
        |> ValueOption.defaultWith (fun _ ->
            raw.GetType().FullName
            |> sprintf "Unknown record value type: %s"
            |> failwith)

module Record =

    let tryFind key (record: IRecord) =
        key
        |> record.Values.TryGetValue
        |> ValueOption.ofPair