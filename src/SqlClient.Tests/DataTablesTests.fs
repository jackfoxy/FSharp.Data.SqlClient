namespace FSharp.Data

open System
open System.Configuration
open System.Transactions
open System.Data.SqlClient
open System.Data
open FSharp.Data
open Xunit

open ProgrammabilityTest

//Tables types structured as: [TypeAlias].[Namespace].Tables.[TableName]
type ShiftTable = AdventureWorks.HumanResources.Tables.Shift
type ProductCostHistory = AdventureWorks.Production.Tables.ProductCostHistory

type DataTablesTests() = 

    let getShiftTableData = AdventureWorks.CreateCommand<"SELECT * FROM HumanResources.Shift", ResultType.DataReader, TypeName = "GetShiftTableData">()

    do
        use cmd = new SqlCommand<"DBCC CHECKIDENT ('HumanResources.Shift', RESEED, 4)", ConnectionStrings.AdventureWorksNamed>()
        cmd.Execute() |> ignore

    let adventureWorks = FSharp.Configuration.AppSettings<"app.config">.ConnectionStrings.AdventureWorks

    member private __.GetRowCount(?conn, ?tran) = 
        let conn = defaultArg conn null
        let tran = defaultArg tran null
        let cmd = AdventureWorks.CreateCommand<"SELECT COUNT(*) FROM HumanResources.Shift", SingleRow = true>(conn, tran)
        cmd.Execute().Value.Value
    
    [<Fact>]
    member __.NewRowAndBulkCopy() = 
        let t = new ShiftTable()
        use conn = new SqlConnection(connectionString = adventureWorks)
        conn.Open()
        use tran = conn.BeginTransaction()
    
        let rows: DataRow[] = 
            [|
                //erased method to provide static typing
                t.NewRow("French coffee break", StartTime = TimeSpan.FromHours 10., EndTime = TimeSpan.FromHours 12., ModifiedDate = Some DateTime.Now.Date)
                t.NewRow("Spanish siesta", TimeSpan.FromHours 13., TimeSpan.FromHours 16., Some DateTime.Now.Date)
            |]
        let bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran)
        let rowsCopied = ref 0L
        bulkCopy.NotifyAfter <- rows.Length
        bulkCopy.SqlRowsCopied.Add(fun args -> rowsCopied := args.RowsCopied)
        //table name is there
        bulkCopy.DestinationTableName <- t.TableName
        bulkCopy.WriteToServer(rows)

        Assert.Equal(int64 rows.Length, !rowsCopied)

    [<Fact
        //(Skip="")
    >]
    member this.AddRowAndBulkCopy() = 
        try
            let t = new ShiftTable()
    
            //erased method to provide static typing
            let now = DateTime.Now.Date
            t.AddRow("French coffee break", StartTime = TimeSpan.FromHours 10., EndTime = TimeSpan.FromHours 12., ModifiedDate = Some now)
            t.AddRow("Spanish siesta", TimeSpan.FromHours 13., TimeSpan.FromHours 16., Some now)
            //check type. Should DateTime not option<DateTime>
            Assert.Equal<DateTime>(now, t.Rows.[0].ModifiedDate)

            let rowsBefore = this.GetRowCount()
        
            //shortcut, convenience method
            t.BulkCopy()

            let rowsAdded = this.GetRowCount() - rowsBefore
            Assert.Equal(t.Rows.Count, rowsAdded)
        finally
            //compenstating tran
            use cmd = new SqlCommand<"
                DELETE FROM HumanResources.Shift WHERE Name IN ('French coffee break', 'Spanish siesta')
            ", ConnectionStrings.AdventureWorksNamed>()
            cmd.Execute() |> ignore

    [<Fact>]
    member this.AddRowAndBulkCopyWithConnOverride() = 
        let t = new ShiftTable()
        use conn = new SqlConnection(connectionString = adventureWorks)
        conn.Open()
        use tran = conn.BeginTransaction()
    
        //erased method to provide static typing
        t.AddRow("French coffee break", StartTime = TimeSpan.FromHours 10., EndTime = TimeSpan.FromHours 12., ModifiedDate = Some DateTime.Now.Date)
        t.AddRow("Spanish siesta", TimeSpan.FromHours 13., TimeSpan.FromHours 16., Some DateTime.Now.Date)

        let rowsBefore = this.GetRowCount(conn, tran)
        
        //shortcut, convenience method
        t.BulkCopy(conn, SqlBulkCopyOptions.Default, tran)

        let rowsAdded = this.GetRowCount(conn, tran) - rowsBefore
        Assert.Equal(t.Rows.Count, rowsAdded)

    [<Fact>]
    member __.DEFAULTConstraint() = 
        let t = new ShiftTable()
        use conn = new SqlConnection(connectionString = adventureWorks)
        conn.Open()
        use tran = conn.BeginTransaction()
    
        //ModifiedDate is not provided
        t.AddRow("French coffee break", StartTime = TimeSpan.FromHours 10., EndTime = TimeSpan.FromHours 12.)
        t.AddRow("Spanish siesta", TimeSpan.FromHours 13., TimeSpan.FromHours 16.)

        //remove ModifiedDate column therefore bulk insert won't send explicit NULLs to server
        t.Columns.Remove(t.ModifiedDateColumn)

        let bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran)
        let rowsCopied = ref 0L
        bulkCopy.NotifyAfter <- t.Rows.Count
        bulkCopy.SqlRowsCopied.Add(fun args -> rowsCopied := args.RowsCopied)
        bulkCopy.DestinationTableName <- t.TableName
        bulkCopy.WriteToServer(t)

        Assert.Equal(int64 t.Rows.Count, !rowsCopied)

    [<Fact>]
    member __.DEFAULTConstraintInsertViaSqlDataAdapter() = 
        let t = new ShiftTable()
        Assert.True t.ModifiedDateColumn.AllowDBNull
        use conn = new SqlConnection(connectionString = adventureWorks)
        conn.Open()
        use tran = conn.BeginTransaction()
    
        //ModifiedDate is not provided
        t.AddRow("French coffee break", StartTime = TimeSpan.FromHours 10., EndTime = TimeSpan.FromHours 12.)
        let yesterday = DateTime.Today.AddDays -1.
        t.AddRow("Spanish siesta", TimeSpan.FromHours 13., TimeSpan.FromHours 16., ModifiedDate = Some yesterday)

        //removing ModifiedDate column is not required as oppose to bulk insert 
        let rowsInserted = t.Update(conn, tran)
        let latestIdentity = 
            use cmd = new SqlCommand<"SELECT IDENT_CURRENT (@tableName)", ConnectionStrings.AdventureWorksNamed, SingleRow = true>()
            cmd.Execute( t.TableName) |> Option.get |> Option.get |> Convert.ToByte

        Assert.Equal(t.Rows.Count, rowsInserted)

        //identity values retrived
        Assert.Equal(t.Rows.[1].ShiftID, latestIdentity)
        Assert.Equal(t.Rows.[0].ShiftID, latestIdentity - 1uy)

        //default values
        Assert.Equal(t.Rows.[1].ModifiedDate, yesterday)
        let serverDate = //because Azure in UTC
            use cmd = new SqlCommand<"SELECT GetDate()", ConnectionStrings.AdventureWorksNamed, SingleRow = true>()
            cmd.Execute().Value
        Assert.Equal(t.Rows.[0].ModifiedDate.Date, serverDate.Date)

    [<Fact>]
    member __.UpdatesPlusAmbientTransaction() = 
        
        use tran = new TransactionScope()
            
        let t = new ShiftTable()
        getShiftTableData.Execute() |> t.Load

        let eveningShift = t.Rows |> Seq.find (fun row -> row.Name = "Evening")
        let finishBy10 = TimeSpan(22, 0, 0)
        Assert.NotEqual(finishBy10, eveningShift.EndTime)
        eveningShift.EndTime <- finishBy10
    
        let rowsUpdated = t.Update()
        Assert.Equal(1, rowsUpdated)

        use getShift = new SqlCommand<"SELECT * FROM HumanResources.Shift", ConnectionStrings.AdventureWorksNamed>()
        let eveningShiftIinDb = getShift.Execute() |> Seq.find (fun x -> x.Name = "Evening")
        Assert.Equal(finishBy10, eveningShiftIinDb.EndTime)

    [<Fact>]
    member __.TableTypeTag() = 
        Assert.Equal<string>(ConnectionStrings.AdventureWorksNamed, AdventureWorks.Commands.GetShiftTableData.ConnectionStringOrName)

    [<Fact>]
    member __.NullableDateTimeColumn() = 

        let table = new ProductCostHistory()
        use cmd = AdventureWorks.CreateCommand<"SELECT * FROM Production.ProductCostHistory WHERE EndDate IS NOT NULL", ResultType.DataReader>()
        cmd.Execute() |> table.Load
        
        Assert.NotEmpty(table.Rows)

        let row = table.Rows.[0]

        Assert.True(row.EndDate.IsSome)
        //dymanic accessor
        Assert.NotEqual(box DBNull.Value, row.["EndDate"])

        row.EndDate <- None

        Assert.True(row.EndDate.IsNone)

    [<Fact>]
    member __.SqlCommandTableInsert() = 
        use cmd = AdventureWorks.CreateCommand<"SELECT Name, StartTime, EndTime FROM HumanResources.Shift", ResultType.DataTable>()
        let t = cmd.Execute()
        use conn = new SqlConnection(connectionString = adventureWorks)
        conn.Open()
        use tran = conn.BeginTransaction()
    
        let row = t.NewRow()
        row.Name <- "French coffee break"
        row.StartTime <- TimeSpan.FromHours 10.
        row.EndTime <- TimeSpan.FromHours 12.
        t.Rows.Add row
        let rowsInserted = t.Update(conn, tran)
        Assert.Equal(1, rowsInserted)

    [<Fact>]
    member __.SqlCommandTableUpdate() = 
        use cmd = AdventureWorks.CreateCommand<"SELECT ShiftID, Name, StartTime, EndTime, ModifiedDate FROM HumanResources.Shift", ResultType.DataTable>()
        let t = cmd.Execute()
        use conn = new SqlConnection(connectionString = adventureWorks)
        conn.Open()
        use tran = conn.BeginTransaction()
    
        let row = t.Rows.[0]
        row.ModifiedDate <- DateTime.Now.Date
        let rowsAffected = t.Update(conn, tran)
        Assert.Equal(1, rowsAffected)

    [<Fact>]
    member this.NewRowAndBulkCopyWithTrsansactionScope() = 
        try
            use tran = new TransactionScope()
            let t = new ShiftTable()
    
            //erased method to provide static typing
            let now = DateTime.Now.Date
            t.AddRow("French coffee break", StartTime = TimeSpan.FromHours 10., EndTime = TimeSpan.FromHours 12., ModifiedDate = Some now)
            t.AddRow("Spanish siesta", TimeSpan.FromHours 13., TimeSpan.FromHours 16., Some now)

            //check type. Should DateTime not option<DateTime>
            Assert.Equal<DateTime>(now, t.Rows.[0].ModifiedDate)

            let rowsBefore = this.GetRowCount()
        
            //shortcut, convenience method
            t.BulkCopy()

            let rowsAdded = this.GetRowCount() - rowsBefore
            Assert.Equal(t.Rows.Count, rowsAdded)
            
            tran.Complete()
        finally
            //compenstating tran
            let t2 = new ShiftTable()
            getShiftTableData.Execute() |> t2.Load
            for r in t2.Rows do
                if r.Name = "French coffee break" || r.Name = "Spanish siesta"
                then 
                    r.Delete()
            let rowsAffected = t2.Update()
            assert (rowsAffected = 2)

    [<Fact>]
    member __.ColumnWithSpaceInNameAndDefaultValue() =
        use tran = new TransactionScope()
        let t = new AdventureWorks.dbo.Tables.TableHavingColumnNamesWithSpaces()
        t.AddRow()
        Assert.Equal(1, t.Update())