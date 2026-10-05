package pe.edu.vallegrande.amankay.contracts;

/**
 * Etapas del pipeline de procesamiento de datos en arquitectura Medallion de Amankay.
 * El flujo ordenado es: BRONZE -> VALIDATION -> QUARANTINE -> SILVER -> QUALITY_GATE -> GOLD -> EXPORT.
 */
public enum PipelineStage {

    BRONZE,
    VALIDATION,
    QUARANTINE,
    SILVER,
    QUALITY_GATE,
    GOLD,
    EXPORT

}
