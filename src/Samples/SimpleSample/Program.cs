using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleSample
{
    using System.Runtime.Serialization;

    using Softweyr.EventStore;
    using Softweyr.EventStore.Hydration.AutoMapper;
    using Softweyr.EventStore.Persistence.InMemory;

    class Program
    {
        static void Main(string[] args)
        {
            // Set up
            var persistenceMethod = new InMemoryPersistenceMethod();
            var eventStore = new EventStore(persistenceMethod);

            // Pre-generate some id's for our streams.
            var stream1Id = Guid.NewGuid();
            var stream2Id = Guid.NewGuid();

            // Write-only sample.
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

            // Read-only model
            foreach (var eventStream in eventStore.GetAll())
            {
                WriteStreamToConsole(eventStream);
            }

            // Read & Write Sample
            using (var session = eventStore.NewSession())
            {
                var eventStream = session.GetById(stream1Id);
                WriteStreamToConsole(eventStream);
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

    public class TheHelloWorldEvent
    {
        // Events *should* be immutable, so private set is recommended.
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
    