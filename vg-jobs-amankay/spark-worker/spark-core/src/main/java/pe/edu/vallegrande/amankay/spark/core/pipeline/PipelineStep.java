package pe.edu.vallegrande.amankay.spark.core.pipeline;

import pe.edu.vallegrande.amankay.contracts.PipelineStage;

/**
 * Contrato para cada etapa ejecutable en el pipeline Medallion.
 */
public interface PipelineStep {

    PipelineStage stage();

    String description();

    StepResult execute(PipelineContext context) throws Exception;
}
