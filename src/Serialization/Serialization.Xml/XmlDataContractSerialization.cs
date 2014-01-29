namespace Softweyr.EventStore.Serialization.XmlDataContract
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;
    using System.Xml;

    using Softweyr.EventStore.Persistence.SqlServer2008;

    public class XmlDataContractSerialization : ISerializationMethod
    {
        private readonly ConcurrentDictionary<Guid, Type> types;

        public XmlDataContractSerialization()
        {
            AppDomain.CurrentDomain.AssemblyLoad += CurrentDomain_AssemblyLoad;
            var potentialTypes =
                AppDomain.CurrentDomain.GetAssemblies().SelectMany(asm => asm.GetTypes()).Where(
                    type => type.GetCustomAttributes(typeof(GuidAttribute), true).Any()).Select(
                        type => new KeyValuePair<Guid, Type>(type.GUID, type)).GroupBy(kvp => kvp.Key).Select(grouping => grouping.First());
            this.types = new ConcurrentDictionary<Guid, Type>(potentialTypes);
        }

        void CurrentDomain_AssemblyLoad(object sender, AssemblyLoadEventArgs args)
        {
            foreach (var type in args.LoadedAssembly.GetTypes().Where(
                    type => type.GetCustomAttributes(typeof(GuidAttribute), true).Any()))
            {
                this.types.TryAdd(type.GUID, type);
            }
        }

        public SerializedData Serialize(object @event)
        {
            var memoryStream = new MemoryStream();
            var serializer = new DataContractSerializer(@event.GetType());
            serializer.WriteObject(memoryStream, @event);
            memoryStream.Seek(0, SeekOrigin.Begin);
            return new XmlSerializedData(@event.GetType().GUID, XmlReader.Create(memoryStream));
        }

        public object Deserialize(SerializedData stream)
        {
            var data = (XmlSerializedData)stream;
            var type = types[data.TypeId];
            var serializer = new DataContractSerializer(type);
            return serializer.ReadObject(data.Reader);
        }
    }
}