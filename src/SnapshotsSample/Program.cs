namespace SimpleSample
{
    using System;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;
    using System.Text;

    using Softweyr.EventStore;
    using Softweyr.EventStore.Persistence.SqlServer2008;
    using Softweyr.EventStore.Serialization.XmlDataContract;

    class Program
    {
        static void Main(string[] args)
        {
            // Set up
            var connectionString =
                "Application Name=EventStore;Integrated Security=SSPI;Data Source=.\\SQL2008R2;Initial Catalog=temp_db";
            var databaseName = "EventStoreSample_" + DateTime.Now.ToString("yyyyMMddHHmmss");
            var serializationMethod = new XmlDataContractSerialization();

            var persistenceMethod = new SqlServer2008PersistenceMethod(connectionString, databaseName, serializationMethod);
            var eventStore = new EventStore(persistenceMethod);

            // Pre-generate some id's for our streams.
            var stream1Id = Guid.NewGuid();
            var stream2Id = Guid.NewGuid();

            // Place some sample data into the streams.
            using (var session = eventStore.NewSession())
            {
                // Adding an event to an event stream that doesn't exist implicitly creates the stream.
                session.AddEvent(stream1Id, new TheHelloWorldEvent("Hello World"));
                session.AddEvent(stream1Id, new TheHelloWorldEvent("Hello World Again"));
                session.AddEvent(stream2Id, new TheHelloWorldEvent("Hello World Again"));

                // Mark the session as successfully completed.
                session.Complete();
                // When the session is disposed, if it is marked as completed it will persist the events.
            }

            using (var session = eventStore.NewSession())
            {
                // Flatten the existing events into a single "snapshot" event.
                var eventStream = session.GetById(stream1Id);
                var sb = new StringBuilder();
                foreach (var @event in eventStream.Events)
                {
                    sb.AppendLine(@event.ToString());
                }

                // Add a snapshot event.
                session.AddSnapshot(stream2Id, new TheHelloWorldEvent(sb.ToString()));
                session.Complete();
            }

            Console.WriteLine("Press any key to continue.");
            Console.ReadLine();
        }

        private static void WriteStreamToConsole(EventStream eventStream)
        {
            Console.WriteLine("EventStreamId: " + eventStream.Id);
            Console.WriteLine("Committed Version: " + eventStream.CommittedVersion);
            Console.WriteLine("Uncommitted Version: " + eventStream.UncommittedVersion);
            foreach (var @event in eventStream.Events)
            {
                Console.WriteLine("\tEvent: " + @event);
            }
        }
    }

    [DataContract] // When using XML serialization, the event must be a data contract.
    [Guid("363CE642-615A-4810-A69B-76A65D0A55BD")] // All events require a guid to identify the type.
    public class TheHelloWorldEvent
    {
        // Events *should* be immutable, so private set is recommended.
        [DataMember]
        public string HelloWorldString { get; private set; }

        public TheHelloWorldEvent(string helloWorldString)
        {
            HelloWorldString = helloWorldString;
        }

        public override string ToString()
        {
            return this.HelloWorldString;
        }
    }
}