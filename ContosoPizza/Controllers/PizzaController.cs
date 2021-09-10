using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using ContosoPizza.Models;
using ContosoPizza.Services;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using Elastic.Apm;
using Elastic.Apm.Api;
using System.Text;

namespace ContosoPizza.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class PizzaController : ControllerBase
    {
        public PizzaController()
        {
        }

        [HttpGet]
        public ActionResult<List<Pizza>> GetAll() =>
            PizzaService.GetAll();

        [HttpGet("{id}")]
        public ActionResult<Pizza> Get(int id)
        {
            var pizza = PizzaService.Get(id);

            if (pizza == null)
                return NotFound();

            return pizza;
        }

        [HttpPost]
        public IActionResult Create(Pizza pizza)
        {

            var pConfig = new ProducerConfig
            {
                BootstrapServers = "pkc-epwny.eastus.azure.confluent.cloud:9092",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "XTSLXLHZOLQLOLRF",
                SaslPassword = "jLsz23wonNID0Eic1hSeCRLhL9nySQkPsoNkGCiVJm17tz+6RkmjlhKHg3vdl/je"
            };
          
            using (var producer = new ProducerBuilder<string, string>(pConfig).Build())
            {
                // Get and Serialize Trace Header from transaction
                var distributedTracingData = Agent.Tracer.CurrentSpan?.OutgoingDistributedTracingData
                    ?? Agent.Tracer.CurrentTransaction?.OutgoingDistributedTracingData;
                var traceParent = distributedTracingData?.SerializeToString();
                Console.WriteLine($"Trace Parent: {traceParent}");

                // Set Kafka Topic and Message Key
                var topic = "chr1";
                var key = "PIZZA";

                PizzaService.Add(pizza);
                string jsonString = JsonSerializer.Serialize(pizza);

                //Add Distributed Trace Header
                var header = new Headers();
                byte[] traceParentByte = Encoding.ASCII.GetBytes(traceParent);
                header.Add("trace-parent", traceParentByte );

                producer.Produce(topic, new Message<string, string> { Key = key, Value = jsonString, Headers = header },
                (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                               Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                            Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                            }
                        });
                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine($"{jsonString} message was produced to topic {topic}");            
            }
            return CreatedAtAction(nameof(Create), new { id = pizza.Id }, pizza);
        }

        [HttpPut("{id}")]
        public IActionResult Update(int id, Pizza pizza)
        {
            if (id != pizza.Id)
                return BadRequest();

            var existingPizza = PizzaService.Get(id);
            if (existingPizza is null)
                return NotFound();

            PizzaService.Update(pizza);

            return NoContent();
        }

        [HttpDelete("{id}")]
        public IActionResult Delete(int id)
        {
            var pizza = PizzaService.Get(id);

            if (pizza is null)
                return NotFound();

            PizzaService.Delete(id);

            return NoContent();
        }
    }
}