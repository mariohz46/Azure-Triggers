using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Azure.Storage.Queues;
using System.Text.Json;

namespace StorageQueue_TriggerQueue;

public class DocumentoMensaje
{
    public string? nombre {get; set;}
    public string? estado {get; set;}
}

public class EncolarDocumento
{
    private readonly ILogger<EncolarDocumento> _logger;
    private const string connectionString = "UseDevelopmentStorage=true";
    private const string nombreCola = "cola-documento";

    public EncolarDocumento(ILogger<EncolarDocumento> logger)
    {
        _logger = logger;
    }

    [Function("EncolarDocumento")]
    public async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post",Route ="encolar")] HttpRequest req)
    {
        var documento = await req.ReadFromJsonAsync<DocumentoMensaje>();
        if(documento is null || string.IsNullOrWhiteSpace(documento.nombre))
        {
            return new BadRequestObjectResult("El campo nombre es obligatorio");
        }
        //Lo que esta dentro de esta seccion de comentario. Si lo utilizare en el proceso de la API real
        var queueClient = new QueueClient(connectionString,nombreCola);
        await queueClient.CreateIfNotExistsAsync();
        
        string mensajeJSON = JsonSerializer.Serialize(documento);
        await queueClient.SendMessageAsync(mensajeJSON);
        //Aqui finaliza la seccion 
        _logger.LogInformation($"Mensaje encolado :{mensajeJSON}");

        return new OkObjectResult($"Documento'{documento.nombre}'encolado correctamente.");
    }
}
