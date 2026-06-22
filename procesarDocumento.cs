using System;
using System.Text.Json;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace StorageQueue_TriggerQueue;

public class procesarDocumento
{
    private readonly ILogger<procesarDocumento> _logger;

    public procesarDocumento(ILogger<procesarDocumento> logger)
    {
        _logger = logger;
    }

    [Function(nameof(procesarDocumento))]
    public void Run([QueueTrigger("cola-documento", Connection = "AzureWebJobsStorage")] QueueMessage message)
    {
        _logger.LogInformation($"Mensaje recibido (intento #{message.DequeueCount}): {message.MessageText}");

        DocumentoMensaje? documento; //variable del tipo DocumentoMensaje. No es una instancia.
        try
        {
            documento = JsonSerializer.Deserialize<DocumentoMensaje>(message.MessageText);
        }
        catch (JsonException ex)
        {
            _logger.LogError($"Error al deserializar el mensaje:{ex.Message}");
            return;
        }

        if (documento is null)
        {
            _logger.LogWarning("El mensaje llego vacio");
            return;
        }

        switch (documento.estado)
        {
            case "Aprobado":
                _logger.LogInformation($"APROBADO: {documento.nombre} -> Insertando en SQL");
                break;
            case "Rejected":
                _logger.LogInformation($"REJECTED: {documento.nombre} -> Enviando notificacion de rechazo");
                break;
            default:
                _logger.LogWarning($"Estado desconocido'{documento.estado}'para el documento{documento.nombre}");
                break;
        }

    }
}