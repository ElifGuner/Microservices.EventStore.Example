using EventStore.Client;
using System;
using System.Text.Json;

#region tutorial
/*
string connectionString = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false";
var settings = EventStoreClientSettings.Create(connectionString);
var client = new EventStoreClient(settings);

//Event oluşturma ve stream'e gönderme:
OrderPlacedEvent orderPlacedEvent = new()
{
    OrderId = 1,
    TotalAmount = 1000
};

EventData eventData = new(
    eventId : Uuid.NewUuid(),
    type: orderPlacedEvent.GetType().Name,
    data:JsonSerializer.SerializeToUtf8Bytes(orderPlacedEvent)
);

client.AppendToStreamAsync(
    streamName:"order-stream",
    expectedState:StreamState.Any,
    eventData: new[] { eventData }
    );

//stream okuma
var events = client.ReadStreamAsync(
    streamName: "order-stream",
    direction: Direction.Forwards, // ileri doğru oku
    revision: StreamPosition.Start // Baştan başla
    );

var datas = await events.ToListAsync();

//subscription
//resolvedEvent: subscribe'ı tetikleyecek olan event
await client.SubscribeToStreamAsync(
    streamName : "order-stream",
    start : FromStream.Start,
    eventAppeared: async(streamSubscription, resolvedEvent, cancellationToken) =>
    {
        OrderPlacedEvent @event = JsonSerializer.Deserialize<OrderPlacedEvent>(resolvedEvent.Event.Data.ToArray());
        await Console.Out.WriteLineAsync(JsonSerializer.Serialize(@event));
    },
    subscriptionDropped:(streamSubscription, subscriptionDroppedReason, exception) => Console.WriteLine("Disconnected")
    );

Console.Read();
class OrderPlacedEvent
{ 
    public int OrderId { get; set; }
    public int TotalAmount { get; set; }
}
*/
#endregion

#region Bakiye Örnek

//eventlere subscribe olarak işleyip bakiye hesaplayacağız.
//----eventler -> object initialize ile içlerini doldur
AccountCreatedEvent accountCreatedEvent = new()
{
    AccountId = "12345",
    CustomerId = "6789",
    StartBalance = 0,
    Date = DateTime.UtcNow
};
MoneyDepositedEvent moneyDepositedEvent1 = new() 
{
    AccountId = "12345",
    Amount = 1000,
    Date = DateTime.UtcNow
};

MoneyDepositedEvent moneyDepositedEvent2 = new()
{
    AccountId = "12345",
    Amount = 500,
    Date = DateTime.UtcNow
};

MoneyWithdrownEvent moneyWithdrownEvent = new()
{
    AccountId = "12345",
    Amount = 200,
    Date = DateTime.UtcNow
};

MoneyDepositedEvent moneyDepositedEvent3 = new()
{
    AccountId = "12345",
    Amount = 50,
    Date = DateTime.UtcNow
};

MoneyTransferredEvent moneyTransferredEvent1 = new() 
{
    AccountId = "12345",
    TargetAccountId = "54321",
    Amount = 250,
    Date = DateTime.UtcNow
};

MoneyTransferredEvent moneyTransferredEvent2 = new()
{
    AccountId = "12345",
    TargetAccountId = "54321",
    Amount = 150,
    Date = DateTime.UtcNow
};

MoneyDepositedEvent moneyDepositedEvent4 = new()
{
    AccountId = "12345",
    Amount = 2000,
    Date = DateTime.UtcNow
};

EventStoreService eventStoreService = new EventStoreService();

//elimizdeki tüm eventleri Event Store'a gönderiyoruz
await eventStoreService.AppendToStreamAsync(
    streamName: $"customer-{accountCreatedEvent.CustomerId}-stream",  //customer seviyesinde aggregate oluşturduk.
    new[] {
        eventStoreService.GenerateEventData(accountCreatedEvent),
        eventStoreService.GenerateEventData(moneyDepositedEvent1),
        eventStoreService.GenerateEventData(moneyDepositedEvent2),
        eventStoreService.GenerateEventData(moneyWithdrownEvent),
        eventStoreService.GenerateEventData(moneyDepositedEvent3),
        eventStoreService.GenerateEventData(moneyTransferredEvent1),
        eventStoreService.GenerateEventData(moneyTransferredEvent2),
        eventStoreService.GenerateEventData(moneyDepositedEvent4)
         }
    );

BalanceInfo balanceInfo = new();
// Bu eventlere subscribe olarak bakiyenin nihai değerini elde edelim.
await eventStoreService.SubscribeToStreamAsync(
     streamName: $"customer-{accountCreatedEvent.CustomerId}-stream",
     eventAppeared: async (streamSubscription, resolvedEvent, cancellationToken) =>
     {
         string eventType = resolvedEvent.Event.EventType;
         object @event = JsonSerializer.Deserialize(resolvedEvent.Event.Data.ToArray(), Type.GetType(eventType)); // type metinsel olduğu için <> değil de , 2. parametrede bu şekilde geçtik.

         switch (@event)
         {
             case AccountCreatedEvent e:
                 balanceInfo.AccountId = e.AccountId;
                 balanceInfo.Balance = e.StartBalance;
                 break;
             case MoneyDepositedEvent e:
                 balanceInfo.Balance += e.Amount;
                 break;
             case MoneyWithdrownEvent e:
                 balanceInfo.Balance -= e.Amount;
                 break;
             case MoneyTransferredEvent e:
                 balanceInfo.Balance -= e.Amount;
                 break;
         }
     }
     );

await Console.Out.WriteLineAsync("*********Balance***Son durum 2950 tl olacak *********");
await Console.Out.WriteLineAsync(JsonSerializer.Serialize(balanceInfo));
await Console.Out.WriteLineAsync("*********Balance************");


//Eventleri işleyen service
class EventStoreService
{
    //bağlantı
    EventStoreClientSettings GetEventStoreClientSettings(string connectionString = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false")
        => EventStoreClientSettings.Create(connectionString);

    //client'ı ne zaman çağırırsam bana event store'a bağlanmış bir event store nesnesi vermiş olacak.
    EventStoreClient Client { get => new EventStoreClient(GetEventStoreClientSettings()); }

    public async Task AppendToStreamAsync(string streamName, IEnumerable<EventData> eventData)
        => await Client.AppendToStreamAsync(
                            streamName: streamName,
                            expectedState: StreamState.Any,
                            eventData: eventData
                            );

    public EventData GenerateEventData(object @event)
        => new EventData(
                eventId: Uuid.NewUuid(),
                type:@event.GetType().Name,
                data: JsonSerializer.SerializeToUtf8Bytes(@event)
            );

    public async Task SubscribeToStreamAsync(string streamName, Func<StreamSubscription, ResolvedEvent, CancellationToken, Task> eventAppeared)
        => Client.SubscribeToStreamAsync(
                streamName: streamName,
                start: FromStream.Start,
                eventAppeared: eventAppeared,
                subscriptionDropped: (x, y, z) => Console.WriteLine("Disconnected")
            );
  
}

class BalanceInfo
{
    public string AccountId { get; set; }
    public int Balance { get; set; }
}
class AccountCreatedEvent
{ 
    public string AccountId { get; set; }
    public string CustomerId { get; set; }
    public int StartBalance { get; set; }
    public DateTime Date { get; set; }
}

class MoneyDepositedEvent
{
    public string AccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}

class MoneyWithdrownEvent
{
    public string AccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}

class MoneyTransferredEvent
{
    public string AccountId { get; set; }
    public string TargetAccountId { get; set; }
    public int Amount { get; set; }
    public DateTime Date { get; set; }
}

#endregion
