(**
    Use cases
*)
#r @"..\..\packages\FSharp.Configuration.0.5.3\lib\net40\FSharp.Configuration.dll"
#r "../../bin/FSharp.Data.SqlClient.dll"
open System
open FSharp.Data
#load "Connection.fs"

[<Literal>] 
let connectionString = ConnectionStrings.AdventureWorksLiteral

[<Literal>]
let queryProductsSql = "
SELECT TOP (@top) Name AS ProductName, SellStartDate, Size
FROM Production.Product 
WHERE SellStartDate > @SellStartDate
"

type QueryProducts = SqlCommandProvider<queryProductsSql, connectionString>

//Custom record types and connection string override
do 
    let cmd = new QueryProducts(connectionString = ConnectionStrings.AdventureWorksLiteral)
    let result = cmd.AsyncExecute(top = 7L, SellStartDate = System.DateTime.Parse "2002-06-01")
    result |> Async.RunSynchronously |> Seq.iter (fun x -> printfn "Product name: %s. Sells start date %A, size: %A" x.ProductName x.SellStartDate x.Size)
    let records = cmd.Execute(top = 7L, SellStartDate = System.DateTime.Parse "2002-06-01") |> List.ofSeq
    records |> Seq.iter (printfn "%A")
    let record = records.Head
    //Record constructor
    let newrecord = QueryProducts.Record("foo", System.DateTime(2000,1,1), Some "bar")
    printfn "%A" (record <> newrecord)


//Two parallel executions
do 
    let cmd = new SqlCommandProvider<queryProductsSql, connectionString>()
    let reader1 = cmd.AsyncExecute(top = 7L, SellStartDate = System.DateTime.Parse "2002-06-01")
    let reader2 = cmd.AsyncExecute(top = 7L, SellStartDate = System.DateTime.Parse "2002-06-01")
    reader1 |> Async.RunSynchronously |> Seq.head |> printfn "%A"
    reader2 |> Async.RunSynchronously |> Seq.head |> printfn "%A"

do 
//Single row hint and optional output columns. Records result type.
    use cmd = new SqlCommandProvider<"SELECT * FROM dbo.ufnGetContactInformation(@PersonId)", connectionString, SingleRow = true>()
    let result = cmd.AsyncExecute(PersonId = 2) 
    result |> Async.RunSynchronously |> Option.get |> fun x -> printfn "Person info: Id - %i, FirstName - %O, LastName - %O, JobTitle - %O, BusinessEntityType - %O" x.PersonID x.FirstName x.LastName x.JobTitle x.BusinessEntityType
    cmd.Execute(PersonId = 2).Value |> fun x -> printfn "Person info: Id - %i, FirstName - %O, LastName - %O, JobTitle - %O, BusinessEntityType - %O" x.PersonID x.FirstName x.LastName x.JobTitle x.BusinessEntityType

//Single row hint and optional output columns. Tuple result type.
do 
    use cmd = new SqlCommandProvider<"SELECT * FROM dbo.ufnGetContactInformation(@PersonId)", connectionString, SingleRow = true>()
    let result = cmd.AsyncExecute( PersonId = 2) 
    result 
        |> Async.RunSynchronously 
        |> Option.map (fun x -> x.PersonID, x.FirstName, x.LastName, x.JobTitle, x.BusinessEntityType)
        |> Option.get 
        |> fun(personId, firstName, lastName, jobTitle, businessEntityType) -> printfn "Person info: Id - %i, FirstName - %O, LastName - %O, JobTitle - %O, BusinessEntityType - %O" personId firstName lastName jobTitle businessEntityType

    cmd.Execute(PersonId = 2)
    |> Option.map (fun x -> x.PersonID, x.FirstName, x.LastName, x.JobTitle, x.BusinessEntityType)
    |> Option.get 
    |> fun(personId, firstName, lastName, jobTitle, businessEntityType) -> printfn "Person info: Id - %i, FirstName - %O, LastName - %O, JobTitle - %O, BusinessEntityType - %O" personId firstName lastName jobTitle businessEntityType

//Single row hint and optional output columns. Single value.
do 
    use cmd = new SqlCommandProvider<"SELECT FirstName FROM dbo.ufnGetContactInformation(@PersonId)", connectionString, SingleRow = true>()
    let result = cmd.AsyncExecute(PersonId = 2) 
    result |> Async.RunSynchronously |> (function | Some(Some firstName) -> printfn "FirstName - %s" firstName | _ -> printfn "Nothing to print" )
    cmd.Execute(PersonId = 2) |> (function | Some(Some firstName) -> printfn "FirstName - %s" firstName | _ -> printfn "Nothing to print" )

//Single value
do 
    use cmd = new SqlCommandProvider<"IF @IsUtc = CAST(1 AS BIT) SELECT GETUTCDATE() ELSE SELECT GETDATE()", connectionString, SingleRow = true>()
    let result = cmd.AsyncExecute(IsUtc = true) 
    result |> Async.RunSynchronously |> printfn "%A"
    cmd.Execute(IsUtc = false) |> printfn "%A"

//Non-query
do
    use cmd = new SqlCommandProvider<"EXEC HumanResources.uspUpdateEmployeePersonalInfo @BusinessEntityID, @NationalIDNumber,@BirthDate, @MaritalStatus, @Gender ", connectionString>()
    let result = cmd.AsyncExecute(BusinessEntityID = 2, NationalIDNumber = "245797967", BirthDate = System.DateTime(1965, 09, 01), MaritalStatus = "S", Gender = "F") 
    let rowsAffected = result |> Async.RunSynchronously 
    use cmd2 = new SqlCommandProvider<"EXEC HumanResources.uspUpdateEmployeePersonalInfo @BusinessEntityID, @NationalIDNumber,@BirthDate, @MaritalStatus, @Gender ", connectionString>()
    cmd2.Execute(BusinessEntityID = 2, NationalIDNumber = "245797967", BirthDate = System.DateTime(1965, 09, 01), MaritalStatus = "S", Gender = "M") |> printfn "%S" 

//Command from file
do 
    SqlCommandProvider<"sampleCommand.sql", connectionString>.Create().Execute() |> Seq.toArray |> printfn "%A"


open System.Security.Principal

do
    use cmd = new SqlCommandProvider<"
        INSERT INTO dbo.ErrorLog
        VALUES (GETDATE(), @UserName, @ErrorNumber, @ErrorSeverity, @ErrorState, @ErrorProcedure, @ErrorLine, @ErrorMessage)", connectionString, SingleRow = true>()

    let user = WindowsIdentity.GetCurrent().Name
    cmd.Execute(user, 121, 16, 3, "insert test", int __LINE__, "failed insert") |> printfn "%A"

#r "Microsoft.SqlServer.Types"

do 
    use cmd = new SqlCommandProvider<"SELECT * FROM HumanResources.Employee WHERE OrganizationLevel = @OrganizationLevel", connectionString>()
    cmd.Execute(2s) |> printfn "%A"

type MyCommand1 = SqlCommandProvider<"SELECT GETDATE() AS Now, GETUTCDATE() AS UtcNow",  ConnectionStrings.LocalHost>
type MyRecord1 = MyCommand1.Record
let r1 = MyCommand1.Record(DateTime.Now, DateTime.UtcNow)

type MyCommand2 = SqlCommandProvider<"SELECT GETDATE() AS Now, GETUTCDATE() AS UtcNow",  ConnectionStrings.LocalHost>
let r2 = MyCommand2.Record(DateTime.Now, DateTime.UtcNow)

type MyRecord = { Now: DateTime; UtcNow: DateTime }

let inline toMyRecord (x: 'Recrod) = 
    {
        Now = (^Record : (member get_Now : unit -> DateTime) x)
        UtcNow = (^Record : (member get_UtcNow : unit -> DateTime) x)
    }

