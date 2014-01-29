namespace Softweyr.EventStore.Persistence.SqlServer2008
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Data.SqlTypes;
    using System.Linq;

    using Microsoft.SqlServer.Server;

    using Softweyr.EventStore.Serialization.XmlDataContract;

    public class SqlServer2008PersistenceSession : IPersistenceSession
    {
        private SqlConnection connection;

        private readonly ISerializationMethod serializationMethod;

        public SqlServer2008PersistenceSession(string connectionString, ISerializationMethod serializationMethod)
        {
            this.serializationMethod = serializationMethod;
            this.connection = new SqlConnection(connectionString);
            this.connection.Open();
        }

        public EventStream GetById(Guid id)
        {
            using (var command = this.connection.CreateCommand())
            {
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "dbo.GetEventsById";
                command.Parameters.Add("@aggregateId", SqlDbType.UniqueIdentifier).Value = id;
                using (var result = command.ExecuteReader())
                {
                    if (!result.HasRows)
                    {
                        return new EventStream(id, 0, 0, new List<object>());
                    }

                    return DeserializeEvents(id, result);
                }
            }
        }

        private EventStream DeserializeEvents(Guid id, SqlDataReader result)
        {
            var lastVersion = 0;
            var snapshotVersion = 0;
            var eventTypeIdColumnId = result.GetOrdinal("EventTypeId");
            var eventDataColumnId = result.GetOrdinal("EventData");
            var versionColumnId = result.GetOrdinal("Version");
            var snapshotVersionColumnId = result.GetOrdinal("SnapshotVersion");
            var events = new List<object>();
            while (result.Read())
            {
                var eventTypeId = result.GetSqlGuid(eventTypeIdColumnId).Value;
                lastVersion = result.GetSqlInt32(versionColumnId).Value;
                snapshotVersion = result.GetSqlInt32(snapshotVersionColumnId).Value;
                events.Add(this.serializationMethod.Deserialize(new XmlSerializedData(eventTypeId, result.GetSqlXml(eventDataColumnId).CreateReader())));
            }

            return new EventStream(id, lastVersion, snapshotVersion, events);
        }

        private object DeserializeQueuedEvent(SqlDataReader result)
        {
            var eventTypeIdColumnId = result.GetOrdinal("EventTypeId");
            var eventDataColumnId = result.GetOrdinal("EventData");
            result.Read();
            var eventTypeId = result.GetSqlGuid(eventTypeIdColumnId).Value;
            return this.serializationMethod.Deserialize(new XmlSerializedData(eventTypeId, result.GetSqlXml(eventDataColumnId).CreateReader()));
        }

        public void Save(EventStream eventStream)
        {
            if (!eventStream.Events.Any())
            {
                return;
            }

            var currentVersion = eventStream.CommittedVersion;
            using (var command = this.connection.CreateCommand())
            {
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "dbo.AddEvents";
                command.Parameters.Add("@eventStream", SqlDbType.Structured);
                command.Parameters["@eventStream"].Direction = ParameterDirection.Input;
                command.Parameters["@eventStream"].TypeName = "EventStore";
                command.Parameters["@eventStream"].Value =
                    eventStream.Events.Select(@event => this.ToSqlDataRecord(eventStream.Id, eventStream.CommittedVersion, eventStream.SnapshotVersion, @event, ref currentVersion));
                command.ExecuteNonQuery();
            }
        }

        private SqlDataRecord ToSqlDataRecord(Guid aggregateId, int expectedVersion, int snapshotVersion, object @event, ref int currentVersion)
        {
            using (var serializedData = this.serializationMethod.Serialize(@event))
            {
                var record = new SqlDataRecord(
                    new SqlMetaData("EventStreamId", SqlDbType.UniqueIdentifier),
                    new SqlMetaData("ExpectedVersion", SqlDbType.Int),
                    new SqlMetaData("SnapshotVersion", SqlDbType.Int),
                    new SqlMetaData("Version", SqlDbType.Int),
                    new SqlMetaData("EventTypeId", SqlDbType.UniqueIdentifier),
                    new SqlMetaData("Payload", SqlDbType.Xml));
                var column = 0;
                record.SetGuid(column++, aggregateId);
                record.SetInt32(column++, expectedVersion);
                record.SetInt32(column++, snapshotVersion);
                record.SetInt32(column++, ++currentVersion);
                record.SetGuid(column++, serializedData.TypeId);
                record.SetSqlXml(column++, new SqlXml(((XmlSerializedData)serializedData).Reader)); // TODO: May be a memory leak here.
                return record;
            }
        }

        public IEnumerable<Guid> GetAllIds()
        {
            using (var command = this.connection.CreateCommand())
            {
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "dbo.GetAllEventStreamIdsForType";
                using (var result = command.ExecuteReader())
                {
                    while (result.Read())
                    {
                        yield return result.GetSqlGuid(0).Value;
                    }
                }
            }
        }

        public void Snapshot(Guid id, int expectedVersion, object snapshotEvent)
        {
            using (var serializedData = this.serializationMethod.Serialize(snapshotEvent))
            {
                var currentVersion = expectedVersion;
                using (var command = this.connection.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "dbo.AddEventStreamSnapshot";
                    command.Parameters.Add("@aggregateId", SqlDbType.UniqueIdentifier).Value = id;
                    command.Parameters.Add("@expectedVersion", SqlDbType.Int).Value = expectedVersion;
                    command.Parameters.Add("@version", SqlDbType.Int).Value = ++currentVersion;
                    command.Parameters.Add("@eventTypeId", SqlDbType.UniqueIdentifier).Value = serializedData.TypeId;
                    command.Parameters.Add("@payload", SqlDbType.Xml).Value =
                        new SqlXml(((XmlSerializedData)serializedData).Reader); // TODO: Possible memory leak.
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            try
            {
                this.connection.Close();
            }
            finally
            {
                this.connection.Dispose();
            }
        }
    }
}