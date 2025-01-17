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

            var connectionSettings = new ConnectionSettings(new Uri("http://localhost:9200"))
                .BasicAuthentication("admin", "cGFzc3dvcmQK");

            _client = new OpenSearchClient(connectionSettings);
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
