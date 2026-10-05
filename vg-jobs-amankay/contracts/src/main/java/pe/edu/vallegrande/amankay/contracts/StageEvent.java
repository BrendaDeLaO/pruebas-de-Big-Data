package pe.edu.vallegrande.amankay.contracts;

/**
 * Evento registrado en cada transición de etapa durante la ejecución del pipeline.
 * Se serializa en formato JSON Lines en el archivo events.jsonl.
 *
 * @param stage      Etapa del pipeline
 * @param status     Estado resultante de la etapa
 * @param timestamp  Marca de tiempo ISO-8601
 * @param rows       Cantidad de filas procesadas o evaluadas
 * @param durationMs Duración de la etapa en milisegundos
 * @param sparkJobs  Número acumulado de jobs Spark ejecutados
 * @param detail     Detalle explicativo o mensaje de diagnóstico
 */
public record StageEvent(
        PipelineStage stage,
        StageStatus status,
        String timestamp,
        Long rows,
        Long durationMs,
        Integer sparkJobs,
        String detail
) {
}
