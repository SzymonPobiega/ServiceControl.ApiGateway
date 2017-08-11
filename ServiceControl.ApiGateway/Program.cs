using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace ServiceControl.ApiGateway
{
    class Program
    {
        static void Main(string[] args)
        {
            Start().GetAwaiter().GetResult();
        }

        static async Task Start()
        {
            var gateway = new Gateway("http://localhost:33333/api", "http://localhost:33400/api",
                new[]
                {
                    "http://localhost:33401/api",
                    "http://localhost:33402/api",
                    "http://localhost:33403/api",
                    "http://localhost:33404/api",
                });

            gateway.Start();
            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();
            await gateway.Stop().ConfigureAwait(false);
        }
    }
}