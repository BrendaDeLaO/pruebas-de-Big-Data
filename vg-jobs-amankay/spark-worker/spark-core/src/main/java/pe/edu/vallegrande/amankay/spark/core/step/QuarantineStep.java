package pe.edu.vallegrande.amankay.spark.core.step;

import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.contracts.WorkerProtocol;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;

import java.io.IOException;

/**
 * Etapa QUARANTINE: Persiste en disco (CSV y Parquet) los registros que infringieron
 * reglas de calidad para su posterior auditoría y análisis.
 */
public final class QuarantineStep implements PipelineStep {

    @Override
    public PipelineStage stage() {
        return PipelineStage.QUARANTINE;
    }

    @Override
    public String description() {
        return "Los eventos con observaciones se guardan con su fuente, fila, clave y motivos";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        long rejected = context.publisher().publish(
                WorkerProtocol.QUARANTINE,
                WorkerProtocol.REJECTED_TABLE,
                context.validation().rejected()
        );
        return StepResult.completed(rejected, rejected == 0 ? "Sin eventos en cuarentena" : rejected + " eventos en cuarentena");
    }
}
