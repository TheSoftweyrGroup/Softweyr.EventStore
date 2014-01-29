namespace Softweyr.EventStore.Persistence.SqlServer2008
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Transactions;

    public class SqlServer2008PersistenceMethod : IPersistenceMethod
    {
        private readonly string connectionString;

        private readonly ISerializationMethod serializationMethod;

        public SqlServer2008PersistenceMethod(string connectionString, string databaseName, ISerializationMethod serializationMethod)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "tempdb" };
            using (var ts = new TransactionScope(TransactionScopeOption.Suppress))
            using (var conn = new SqlConnection(connectionStringBuilder.ToString()))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText =
                        string.Format(
                            @"if not exists(select * from sys.databases where name = '{0}')
	CREATE DATABASE {0}", databaseName);
                    command.ExecuteNonQuery();
                }

                conn.ChangeDatabase(databaseName);
                using (var command = conn.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = @"
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Events]') AND type in (N'U'))
CREATE TABLE [dbo].[Events](
	[EventStreamId] [uniqueidentifier] NOT NULL,
	[Version] [int] NOT NULL,
	[EventTypeId] [uniqueidentifier] NOT NULL,
	[EventData] xml NOT NULL
CONSTRAINT [PK_Events] PRIMARY KEY CLUSTERED 
(
	[EventStreamId] ASC,
	[Version] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EventStreams]') AND type in (N'U'))
CREATE TABLE [dbo].[EventStreams](
	[EventStreamId] [uniqueidentifier] NOT NULL,
	[CurrentVersion] [int] NOT NULL,
	[SnapshotVersion] [int] NOT NULL,
 CONSTRAINT [PK_EventStreams] PRIMARY KEY CLUSTERED 
(
	[EventStreamId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EventQueue]') AND type in (N'U'))
CREATE TABLE [dbo].[EventQueue](
	[EventData] xml NOT NULL,
	[EventTypeId] [uniqueidentifier] NOT NULL
) ON [PRIMARY]

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EventTypes]') AND type in (N'U'))
CREATE TABLE [dbo].[EventTypes](
	[EventTypeId] [uniqueidentifier] NOT NULL,
	[Name] [varchar](max) NOT NULL,
 CONSTRAINT [PK_EventTypes] PRIMARY KEY CLUSTERED 
(
	[EventTypeId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

IF NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'EventStore' AND ss.name = N'dbo')
CREATE TYPE [dbo].[EventStore] AS TABLE(
    [EventStreamId] [uniqueidentifier] NOT NULL,
    [ExpectedVersion] [int] NOT NULL,
    [SnapshotVersion] [int] NOT NULL,
	[Version] [int] NOT NULL,
	[EventTypeId] [uniqueidentifier] NOT NULL,
	[Payload] xml NOT NULL
)

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AddEvents]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'
CREATE PROCEDURE [dbo].[AddEvents]
  @eventStream EventStore READONLY
as
BEGIN
  set nocount on;

  DECLARE @aggregates Table(EventStreamId uniqueidentifier, ExpectedVersion int, SnapshotVersion int, NewVersion int)
  INSERT INTO @aggregates (EventStreamId, ExpectedVersion, SnapshotVersion, NewVersion) SELECT EventStreamId, ExpectedVersion, SnapshotVersion, MAX(Version) FROM @eventStream GROUP BY EventStreamId, ExpectedVersion, SnapshotVersion;

  BEGIN TRAN  
  IF (EXISTS (SELECT EventStreamId FROM (SELECT agg.EventStreamId, agg.ExpectedVersion, EventStreams.CurrentVersion FROM @aggregates agg LEFT OUTER JOIN EventStreams WITH (READCOMMITTED, ROWLOCK) ON agg.EventStreamId = EventStreams.EventStreamId) innerTable WHERE innerTable.ExpectedVersion <> COALESCE(innerTable.CurrentVersion, 0)))
  BEGIN
    -- Rollback the transaction
    ROLLBACK
    
    -- Raise an error and return
    RAISERROR(''EventStream has been modified since stream was started.'', 16, 1);
    RETURN -1;
  END
  
  MERGE EventStreams as [target]
  USING @aggregates as [source]
  ON [target].EventStreamID = [source].EventStreamId
  WHEN MATCHED THEN
     UPDATE SET CurrentVersion = source.NewVersion, SnapshotVersion = source.SnapshotVersion
  WHEN NOT MATCHED THEN
     INSERT (EventStreamId, CurrentVersion, SnapshotVersion)
	 VALUES (source.EventStreamId, source.NewVersion, source.SnapshotVersion);

  IF @@ERROR <> 0
  BEGIN
    -- Rollback the transaction
    ROLLBACK

    -- Raise an error and return
    RAISERROR (''Error in adding new aggregates.'', 16, 1)
    RETURN -1;
  END

  INSERT INTO Events (EventStreamId, [Version], [EventTypeId], [EventData]) SELECT EventStreamId, [Version], EventTypeId, payload fROM @eventStream;
  IF @@ERROR <> 0
  BEGIN
    -- Rollback the transaction
    ROLLBACK

    -- Raise an error and return
    RAISERROR (''Error in adding new events.'', 16, 1)
    RETURN -1;
  END
  
  INSERT INTO EventQueue (EventTypeId, [EventData]) SELECT EventTypeId, payload fROM @eventStream;
  IF @@ERROR <> 0
  BEGIN
    -- Rollback the transaction
    ROLLBACK

    -- Raise an error and return
    RAISERROR (''Error in adding new events to queue.'', 16, 1)
    RETURN -1;
  END
  
  COMMIT TRAN
END'
END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AddEventStreamSnapshot]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'
CREATE PROCEDURE AddEventStreamSnapshot
  @aggregateId uniqueidentifier,
  @expectedVersion int,
  @version int,
  @eventTypeId uniqueidentifier,
  @payload xml
as
BEGIN
	set nocount on;
	DECLARE @snapshotEvents EventStore;
	INSERT INTO @snapshotEvents (EventStreamId, ExpectedVersion, [Version], EventTypeId, Payload) VALUES (@aggregateId, @expectedVersion, @version, @eventTypeId, @payload);
	EXEC [AddEvents] @snapshotEvents
	UPDATE [dbo].[EventStreams] SET [SnapshotVersion] = @version WHERE EventStreamId = @aggregateId;
END'
END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetEventsById]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'
CREATE PROCEDURE GetEventsById
	@aggregateId uniqueidentifier
AS
BEGIN
	set nocount on;
	SELECT [Events].[EventStreamId]
      ,[Events].[Version]
      ,[EventStreams].[SnapshotVersion]
      ,[Events].[EventTypeId]
      ,[Events].[EventData]
  FROM [dbo].[Events] WITH (READCOMMITTED) -- READCOMMITTED cause issues?
  INNER JOIN [dbo].[EventStreams] WITH (READCOMMITTED, ROWLOCK) ON [Events].EventStreamId = [EventStreams].EventStreamId And Version >= [EventStreams].[SnapshotVersion]
  WHERE [Events].EventStreamId = @aggregateId
  ORDER BY [dbo].[Events].[Version] ASC;
END'
END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetAllEventStreamIdsForType]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'
CREATE PROCEDURE GetAllEventStreamIdsForType
AS
BEGIN
	set nocount on;
	SELECT EventStreamID FROM EventStreams
END'
END

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DequeueEvent]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'
CREATE PROCEDURE DequeueEvent 
as
  set nocount on;
  delete top(1) from EventQueue with (rowlock, readpast)
      output deleted.[EventData], deleted.EventTypeId;'
END
";
                    command.ExecuteNonQuery();
                }

                conn.Close();
                ts.Complete();
            }

            connectionStringBuilder.InitialCatalog = databaseName;
            this.connectionString = connectionStringBuilder.ToString();
            this.serializationMethod = serializationMethod;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            // Do nothing.
        }

        public IPersistenceSession NewSession()
        {
            return new SqlServer2008PersistenceSession(connectionString, this.serializationMethod);
        }

        public EventStream GetById(Guid id)
        {
            using (var session = this.NewSession())
            {
                return session.GetById(id);
            }
        }

        public void Save(Guid id, int expectedVersion, IEnumerable<object> events)
        {
            throw new NotSupportedException();
        }

        /*
        public void Snapshot(Guid id, int version, object snapshotEvent)
        {
            using (var session = this.NewSession())
            {
                session.Snapshot(id, version, snapshotEvent);
            }
        } */

        public IEnumerable<Guid> GetAllIds()
        {
            using (var session = this.NewSession())
            {
                return session.GetAllIds().ToArray(); // TODO: The ToArray is a bodge as the session is disposed before the next item in the IEnumerable is returned.
            }
        }
    }
}