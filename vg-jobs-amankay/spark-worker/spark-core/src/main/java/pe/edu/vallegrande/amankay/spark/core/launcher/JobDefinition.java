package pe.edu.vallegrande.amankay.spark.core.launcher;

import pe.edu.vallegrande.amankay.contracts.JobType;
import pe.edu.vallegrande.amankay.spark.core.pipeline.Pipeline;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;

import java.util.List;

/**
 * Contrato base para la definición de cualquier Job de procesamiento Spark en Amankay.
 */
public interface JobDefinition {

    JobType type();

    List<PipelineStep> steps();

    default Pipeline pipeline() {
        return new Pipeline(steps());
    }
}
