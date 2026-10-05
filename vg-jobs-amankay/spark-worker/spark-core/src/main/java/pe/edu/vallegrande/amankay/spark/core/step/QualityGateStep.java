package pe.edu.vallegrande.amankay.spark.core.step;

import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.contracts.WorkerProtocol;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.amankay.spark.core.publication.JsonPublisher;
import pe.edu.vallegrande.amankay.spark.core.quality.QualityGate;
import pe.edu.vallegrande.amankay.spark.core.quality.QualityReport;

import java.io.IOException;

/**
 * Etapa QUALITY_GATE: Evalúa las reglas de calidad por tabla contra la tolerancia asignada
 * y escribe quality.json. Si no supera la puerta, bloquea la publicación de Gold.
 */
public final class QualityGateStep implements PipelineStep {

    private final QualityGate gate;

    public QualityGateStep(QualityGate gate) {
        this.gate = gate;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.QUALITY_GATE;
    }

    @Override
    public String description() {
        return "Se evalúa la tasa de rechazo por tabla contra el umbral de tolerancia del job";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        QualityReport report = gate.evaluate(context.validation(), context.maxRejectedRatio());
        context.quality(report);
        JsonPublisher.write(context.output().resolve(WorkerProtocol.QUALITY_FILE), report);
        long rows = context.validation().received();
        return report.published() ? StepResult.completed(rows, report.decision()) : StepResult.blocked(rows, report.decision());
    }
}
