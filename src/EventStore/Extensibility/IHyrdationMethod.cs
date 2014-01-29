namespace Softweyr.EventStore
{
    public interface IHyrdationMethod
    {
        void HydrateInto(object @event, object aggregate);
    }
}