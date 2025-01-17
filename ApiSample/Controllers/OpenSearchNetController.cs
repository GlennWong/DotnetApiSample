using Microsoft.AspNetCore.Mvc;
using OpenSearch.Net;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace ApiSample.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class OpenSearchNetController : ControllerBase
    {
        private readonly ILogger<OpenSearchNetController> _logger;
        private readonly OpenSearchLowLevelClient _client;

        public OpenSearchNetController(ILogger<OpenSearchNetController> logger)
        {
            _logger = logger;

            var connectionPool = new SingleNodeConnectionPool(new Uri("http://localhost:9200"));
            var settings = new ConnectionConfiguration(connectionPool)
                .BasicAuthentication("admin", "cGFzc3dvcmQK");

            _client = new OpenSearchLowLevelClient(settings);
        }

        // Get cluster info
        [HttpGet("info")]
        public IActionResult GetClusterInfo()
        {
            _logger.LogInformation("Retrieving cluster info");

            var response = _client.DoRequest<StringResponse>(OpenSearch.Net.HttpMethod.GET, "/");
            _logger.LogInformation(response.Body);

            return Content(response.Body, "application/json");
        }

        // Create a document
        [HttpPost("{index}/create")]
        public async Task<IActionResult> CreateDocument(string index, [FromBody] JsonElement document)
        {
            _logger.LogInformation($"Creating document in index {index}");

            var response = await _client.IndexAsync<StringResponse>(index, PostData.String(document.ToString()));

            if (!response.Success)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(JsonSerializer.Deserialize<JsonElement>(response.Body));
        }

        // Read a document by ID
        [HttpGet("{index}/{id}")]
        public async Task<IActionResult> GetDocument(string index, string id)
        {
            _logger.LogInformation($"Retrieving document {id} from index {index}");

            var response = await _client.GetAsync<StringResponse>(index, id);

            if (!response.Success)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Content(response.Body, "application/json");
        }

        // Update a document by ID
        [HttpPut("{index}/{id}")]
        public async Task<IActionResult> UpdateDocument(string index, string id, [FromBody] JsonElement document)
        {
            _logger.LogInformation($"Updating document {id} in index {index}");

            var response = await _client.UpdateAsync<StringResponse>(index, id, PostData.String(JsonSerializer.Serialize(new { doc = document })));

            if (!response.Success)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(JsonSerializer.Deserialize<JsonElement>(response.Body));
        }

        // Delete a document by ID
        [HttpDelete("{index}/{id}")]
        public async Task<IActionResult> DeleteDocument(string index, string id)
        {
            _logger.LogInformation($"Deleting document {id} from index {index}");

            var response = await _client.DeleteAsync<StringResponse>(index, id);

            if (!response.Success)
            {
                _logger.LogError(response.DebugInformation);
                return StatusCode((int)(response.HttpStatusCode ?? 500), response.DebugInformation);
            }

            return Ok(JsonSerializer.Deserialize<JsonElement>(response.Body));
        }

        // FIXME
        // Get all indices
[HttpGet("indices")]
public async Task<IActionResult> GetAllIndices()
{
    _logger.LogInformation("Retrieving all indices");

    var response = await _client.DoRequestAsync<StringResponse>(OpenSearch.Net.HttpMethod.GET, "/_cat/indices?v", CancellationToken.None);

    if (!response.Success)
    {
        _logger.LogError(response.DebugInformation);
        return StatusCode((int)(response.HttpStatusCode ?? 500), response.DebugInformation);
    }

    return Content(response.Body, "application/json");
}
    }
}
