using EasyNetQ;

namespace workerservice;

public class MyMessage
{
    public int Id { get; set; }
    public string Text { get; set; }
}

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private IBus _bus;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        this._bus = RabbitHutch.CreateBus("host=localhost;username=admin;password=admin");
        //For test reason
        //_bus.SendReceive.Send<MyMessage>("workerqueue", new MyMessage() { Id = 1, Text = "My Message 1" });
        //_bus.SendReceive.Send<MyMessage>("workerqueue", new MyMessage() { Id = 2, Text = "My Message 2" });
        //_bus.SendReceive.Send<MyMessage>("workerqueue", new MyMessage() { Id = 3, Text = "My Message 3" });

        return base.StartAsync(cancellationToken);
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        this._bus.Dispose();
        return base.StopAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        //For test reason
        //await Task.Delay(5000, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

            Action<MyMessage> action = HandleMessage;
            await this._bus.SendReceive.ReceiveAsync<MyMessage>("workerqueue", action).ConfigureAwait(false);

            await Task.Delay(5000, stoppingToken);
        }
    }

    private static void HandleMessage(MyMessage msg)
    {
        Console.WriteLine(msg.Id + " " + msg.Text);
    }
}

