using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using OpenSearch.Client;
using System.Text.Json;

namespace ApiSample.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class OpenSearchClientController : ControllerBase
    {
        private readonly ILogger<OpenSearchClientController> _logger;
        private readonly IOpenSearchClient _client;

        public OpenSearchClientController(ILogger<OpenSearchClientController> logger)
        {
            _logger = logger;

            var connectionSettings = new ConnectionSettings(new Uri("https://vpc-awsdevus-es-pluto-v5-kzvbmh5mxeemx3jmegaahb6jqu.us-east-1.es.amazonaws.com"))
                //.BasicAuthentication("admin", "cGFzc3dvcmQK");
                .DefaultIndex("es-pluto-index-v2");

            _client = new OpenSearchClient(connectionSettings);
        }

        // Get all indices
        [HttpGet("BulkAsync")]
        public async Task<IActionResult> GetBulkAsync()
        {
            _logger.LogInformation("Testing BulkAsync");

            var documents = new List<object>
            {
                new
                {
                    campaignid = 102938,
                    modifieddate = "01/11/2024 02:05:31 AM",
                    eventType = 2,
                    nonbillablespend = 0.0,
                    dayhour = "2024-01-10T00:00:00",
                    postingid = "9807bb49-607e-4e55-b1a1-96cfdf298ff9",
                    providercode = "adeccoftpin",
                    cpas_enabled_cpc = 0.0,
                    billableclicks = 20,
                    orig_cpc = 0.25,
                    cpc = 0.25,
                    spend = 5.0,
                    contractid = 109,
                    nonbillableclicks = 0,
                    day = "2024-01-10",
                    overage = 0.0
                }
                // Add other documents here
            };

            var bulkRequest = new BulkRequest("es-pluto-index-v2")
            {
                Operations = new List<IBulkOperation>()
            };

            foreach (var doc in documents)
            {
                bulkRequest.Operations.Add(new BulkIndexOperation<object>(doc));
            }

            var response = await _client.BulkAsync(bulkRequest);

            if (response.Errors)
            {
                foreach (var itemWithError in response.ItemsWithErrors)
                {
                    _logger.LogInformation($"Failed to index document ID: {itemWithError.Id}, Error: {itemWithError.Error.Reason}");
                }
            }
            else
            {
                _logger.LogInformation("Bulk operation completed successfully.");
            }

            _logger.LogInformation(response.ToString());

            return StatusCode((int)(response.ApiCall.HttpStatusCode ?? 500), response.DebugInformation);
        }

        // Get all indices
        [HttpGet("indices")]
        public async Task<IActionResult> GetAllIndices()
        {
            _logger.LogInformation("Retrieving all indices");

            var response = await _client.Cat.IndicesAsync();

            if (!response.IsValid)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.ApiCall.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(response.Records);
        }


        // Read a document by ID
        [HttpGet("{index}/{id}")]
        public async Task<IActionResult> GetDocument(string index, string id)
        {
            _logger.LogInformation($"Retrieving document {id} from index {index}");

            var documentPath = new DocumentPath<object>(id).Index(index);
            var response = await _client.GetAsync<object>(documentPath);

            if (!response.IsValid)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.ApiCall.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(response.Source);
        }

        // Create a document
        [HttpPost("{index}")]
        public async Task<IActionResult> CreateDocument(string index, [FromBody] object document)
        {
            _logger.LogInformation($"Creating document in index {index}");

            var response = await _client.IndexAsync(document, i => i.Index(index));

            if (!response.IsValid)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.ApiCall.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(response.Id);
        }


        // Update a document by ID
        [HttpPut("{index}/{id}")]
        public async Task<IActionResult> UpdateDocument(string index, string id, [FromBody] object document)
        {
            _logger.LogInformation($"Updating document {id} in index {index}");

            var response = await _client.UpdateAsync<object>(id, u => u.Index(index).Doc(document));

            if (!response.IsValid)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.ApiCall.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(response.Result);
        }

        // Delete a document by ID
        [HttpDelete("{index}/{id}")]
        public async Task<IActionResult> DeleteDocument(string index, string id)
        {
            _logger.LogInformation($"Deleting document {id} from index {index}");

            var response = await _client.DeleteAsync<object>(id, d => d.Index(index));

            if (!response.IsValid)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.ApiCall.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(response.Result);
        }
    }
}
