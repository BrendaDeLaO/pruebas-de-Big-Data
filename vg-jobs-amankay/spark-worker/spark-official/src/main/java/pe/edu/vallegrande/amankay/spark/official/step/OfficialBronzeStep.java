package pe.edu.vallegrande.amankay.spark.official.step;

import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.spark.core.layer.BronzeTables;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.amankay.spark.official.extraction.OfficialExtractor;

/**
 * Etapa BRONZE: Ingesta de archivos CSV oficiales de emergencias en modo permisivo.
 */
public final class OfficialBronzeStep implements PipelineStep {

    private final OfficialExtractor extractor;

    public OfficialBronzeStep(OfficialExtractor extractor) {
        this.extractor = extractor;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.BRONZE;
    }

    @Override
    public String description() {
        return "Lectura e ingesta de fuentes oficiales como texto preservando datos crudos";
    }

    @Override
    public StepResult execute(PipelineContext context) {
        BronzeTables tables = extractor.read(context.spark(), context.input());
        context.bronze(tables);
        return StepResult.completed(tables.totalRows(), tables.describe());
    }
}
