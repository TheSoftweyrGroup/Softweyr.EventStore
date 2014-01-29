namespace Softweyr.EventStore.Persistence.SqlServer2008
{
    public interface ISerializationMethod
    {
        SerializedData Serialize(object @event);

        object Deserialize(SerializedData serializedData);
    }
}