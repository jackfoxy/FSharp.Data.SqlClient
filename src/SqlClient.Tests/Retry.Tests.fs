module FSharp.Data.RetryTests

open FSharp.Data
open Xunit
open System.Data.SqlClient
open System.Data

type RaiseError = SqlCommandProvider<"THROW 51000, 'Error raised.', 12", ConnectionStrings.AdventureWorksNamed>

[<Fact>]
let ExecuteFailure()  = 
    let wasCalled = ref false
    let number = ref None
    let state = ref None

    let cmd = new RaiseError(error = fun why -> 
        wasCalled := true 
        number := Some why.Number
        state := Some why.State
        false
    )
    let why = Assert.Throws<SqlException>(fun() -> cmd.Execute() |> box)

    Assert.Equal(51000, why.Number)
    Assert.Equal(12uy, why.State)

    Assert.True !wasCalled
    Assert.Equal(Some 51000, !number)
    Assert.Equal(Some 12uy, !state)
    
[<Fact>]
let Retry3Times()  = 
    use conn = new SqlConnection(ConnectionStrings.AdventureWorksLiteral)
    let timesRetried = ref 0
    let retry _ = 
        if !timesRetried < 3 
        then 
            incr timesRetried
            true
        else
            false
    
    let cmd = new RaiseError(error = retry) 
    let why = Assert.Throws<SqlException>(fun() -> cmd.Execute() |> box)

    Assert.Equal( 3, !timesRetried)

[<Fact>]
let RunSuccessfullyThirdTime() =
     
    use conn = new SqlConnection( ConnectionStrings.AdventureWorksLiteral)
    conn.Open()
    let message = ref ""
    conn.InfoMessage.Add(fun x -> message := x.Message)
    let wasCalled = ref false

    let cmd = ref Unchecked.defaultof<_>
    let retry _ = 
        wasCalled := true
        (!cmd :> ISqlCommand).Raw.CommandText <- "PRINT 'Hello world'"
        true
    
    cmd := new RaiseError(conn, error = retry)
         
    cmd.Value.Execute() |> ignore

    Assert.True( !wasCalled)
    Assert.Equal<string>( "Hello world", !message)

[<Fact>]
let RestoreExternalConnectin() =
     
    use conn = new SqlConnection( ConnectionStrings.AdventureWorksLiteral)

    let cmd = ref Unchecked.defaultof<_>
    let retry _ = 
        if conn.State <> ConnectionState.Open
        then 
            conn.Open()
        true
    
    cmd := new SqlCommandProvider<"SELECT 42", ConnectionStrings.AdventureWorksLiteral, SingleRow = true>(error = retry)

    Assert.Equal<_>( Some 42, cmd.Value.Execute())