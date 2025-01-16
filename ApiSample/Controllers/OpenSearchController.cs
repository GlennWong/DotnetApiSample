using Microsoft.AspNetCore.Mvc;
using OpenSearch.Net;
using System;

namespace ApiSample.Controllers;

[ApiController]
[Route("[controller]")]
public class OpenSearchController : ControllerBase
{
    private readonly ILogger<OpenSearchController> _logger;
    private readonly OpenSearchLowLevelClient _client;

    public OpenSearchController(ILogger<OpenSearchController> logger)
    {
        _logger = logger;

        var connectionPool = new SingleNodeConnectionPool(new Uri("http://localhost:9200")); // Replace with your OpenSearch URL
        var settings = new ConnectionConfiguration(connectionPool)
            .BasicAuthentication("admin", "cGFzc3dvcmQK");

        _client = new OpenSearchLowLevelClient(settings);
    }

    // Endpoint to get OpenSearch Info
    [HttpGet("info")]
    public IActionResult GetClusterInfo()
    {
        this._logger.LogInformation("hello ----------");

        // Perform the Info request
        var response = _client.DoRequest<StringResponse>(
            OpenSearch.Net.HttpMethod.GET,
            "/"
        );

        this._logger.LogInformation(response.Body);

        return Ok(response.Body); // Return raw OpenSearch cluster information
    }

    // Endpoint to search in a specific index
    [HttpGet("search/{indexName}")]
    public IActionResult SearchIndex(string indexName)
    {
        // Implementation for searching in a specific index
        return Ok();
    }

}

