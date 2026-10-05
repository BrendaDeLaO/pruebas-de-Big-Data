package pe.edu.vallegrande.amankay.contracts;

/**
 * Estado de ejecución de una etapa del pipeline de procesamiento.
 */
public enum StageStatus {

    RUNNING,
    COMPLETED,
    BLOCKED,
    SKIPPED,
    FAILED

}
