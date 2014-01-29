namespace Softweyr.EventStore.Serialization.BinaryFormatter
{
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;

    using Softweyr.EventStore.Persistence.SqlServer2008;

    public class BinaryDataContractSerialization : ISerializationMethod
    {
        private readonly BinaryFormatter formatter = new BinaryFormatter();

        public SerializedData Serialize(object @event)
        {
            var memoryStream = new MemoryStream();
            this.formatter.Serialize(memoryStream, @event);
            memoryStream.Seek(0, SeekOrigin.Begin);
            return new BinarySerializedData(@event.GetType().GUID, memoryStream);
        }

        public object Deserialize(SerializedData stream)
        {
            var data = (BinarySerializedData)stream;
            return this.formatter.Deserialize(data.Stream);
        }
    }
}