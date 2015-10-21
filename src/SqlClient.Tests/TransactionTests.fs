﻿module FSharp.Data.TransactionTests

open System
open System.Data
open System.Transactions
open System.Data.SqlClient

open Xunit

open FSharp.Data.TypeProviderTest
open FSharp.Data.SqlClient

[<Fact>]
let ``Closing ConnectionStrings.AdventureWorks on complete``() =
    use command = new DeleteBitCoin()
    command.Execute(bitCoinCode) |> ignore
    Assert.Equal(System.Data.ConnectionState.Closed, (command :> ISqlCommand).Raw.Connection.State)

[<Fact>]
let implicit() =
    (new DeleteBitCoin()).Execute(bitCoinCode) |> ignore
    begin
        use tran = new TransactionScope() 
        Assert.Equal(1, InsertBitCoin.Create().Execute(bitCoinCode, bitCoinName))
        Assert.Equal(1, GetBitCoin.Create().Execute(bitCoinCode) |> Seq.length)
        Assert.Equal( Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier)
    end
    Assert.Equal(0, GetBitCoin.Create().Execute(bitCoinCode) |> Seq.length)

[<Fact>]
let implicitWithConnInstance() =
    (new DeleteBitCoin()).Execute(bitCoinCode) |> ignore
    begin
        use tran = new TransactionScope() 
        use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
        conn.Open()
        Assert.Equal(1, InsertBitCoin.Create(conn).Execute(bitCoinCode, bitCoinName))
        Assert.Equal(1, GetBitCoin.Create(conn).Execute(bitCoinCode) |> Seq.length)
        Assert.Equal( Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier)
    end
    Assert.Equal(0, (new GetBitCoin()).Execute(bitCoinCode) |> Seq.length)

[<Fact>]
let implicitAsync() =
    (new DeleteBitCoin()).Execute(bitCoinCode) |> ignore
    begin
        use tran = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled) 
        Assert.Equal(1, (new InsertBitCoin()).AsyncExecute(bitCoinCode, bitCoinName) |> Async.RunSynchronously)
        Assert.Equal(1, (new GetBitCoin()).AsyncExecute(bitCoinCode) |> Async.RunSynchronously |> Seq.length)
        Assert.Equal( Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier)
    end
    Assert.Equal(0, (new GetBitCoin()).Execute(bitCoinCode) |> Seq.length)

[<Fact>]
let implicitAsyncNETBefore451() =
    (new DeleteBitCoin()).Execute(bitCoinCode) |> ignore
    begin
        use tran = new TransactionScope() 
        use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
        conn.Open()
        conn.EnlistTransaction(Transaction.Current)
        let localTran = conn.BeginTransaction()
        Assert.Equal(1, (new InsertBitCoin(conn, localTran)).AsyncExecute(bitCoinCode, bitCoinName) |> Async.RunSynchronously)
        Assert.Equal(1, (new GetBitCoin(conn, localTran)).AsyncExecute(bitCoinCode) |> Async.RunSynchronously |> Seq.length)
        Assert.Equal( Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier)
    end
    Assert.Equal(0, (new GetBitCoin()).Execute(bitCoinCode) |> Seq.length)

[<Fact>]
let local() =
    (new DeleteBitCoin()).Execute(bitCoinCode) |> ignore
    use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
    conn.Open()
    let tran = conn.BeginTransaction()
    Assert.Equal(1, (new InsertBitCoin(conn, tran)).Execute(bitCoinCode, bitCoinName))
    Assert.Equal(1, (new GetBitCoin(conn, tran)).Execute(bitCoinCode) |> Seq.length)
    tran.Rollback()
    Assert.Equal(0, (new GetBitCoin()).Execute(bitCoinCode) |> Seq.length)

type RaiseError = SqlCommandProvider<"THROW 51000, 'Error raised.', 1 ", ConnectionStrings.AdventureWorksNamed>

[<Fact>]
let notCloseExternalConnInCaseOfError() =
    use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
    conn.Open()
    let tran = conn.BeginTransaction()
    use cmd = new RaiseError(conn, tran)
    try
        cmd.Execute() |> ignore
    with _ ->
        Assert.True(conn.State = ConnectionState.Open)

[<Fact>]
let notCloseExternalConnInCaseOfError2() =
    use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
    conn.Open()
    use cmd = new RaiseError(conn)
    try
        cmd.Execute() |> ignore
    with _ ->
        Assert.True(conn.State = ConnectionState.Open)

[<Fact>]
let donNotOpenConnectionOnObject() =
    use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
    use cmd = new SqlCommandProvider<"SELECT 42", ConnectionStrings.AdventureWorksNamed>(conn)
    Assert.Throws<InvalidOperationException>(fun() -> cmd.Execute() |> ignore)    

type NonQuery = SqlCommandProvider<"DBCC CHECKIDENT ('HumanResources.Shift', RESEED, 4)", ConnectionStrings.AdventureWorksNamed>

[<Fact>]
let donNotOpenConnectionOnObjectForNonQuery() =
    use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
    Assert.Throws<InvalidOperationException>(fun() -> NonQuery.Create(conn).Execute()|> ignore)    
    
[<Fact>]
let donNotOpenConnectionOnObjectForAsyncNonQuery() =
    use conn = new SqlConnection(ConnectionStrings.AdventureWorks)
    Assert.Throws<InvalidOperationException>(fun() -> NonQuery.Create(conn).AsyncExecute() |> Async.RunSynchronously |> ignore)    
    
