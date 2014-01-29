namespace Softweyr.EventStore.Hydration.AutoMapper
{
    // using AutoMapper;

    public class AutoMapperHydrationMethod : IHyrdationMethod
    {
        public void HydrateInto(object @event, object aggregate)
        {
            // Mapper.Map(@event, aggregate, @event.GetType(), aggregate.GetType());
        }
    }
}