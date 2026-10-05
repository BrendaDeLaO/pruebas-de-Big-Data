package pe.edu.vallegrande.amankay.spark.core.step;

import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.spark.core.layer.GoldTables;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;

/**
 * Etapa GOLD: Ejecuta los algoritmos de analítica, agregaciones y consolidación
 * sobre los datos Silver validados.
 */
public final class GoldStep implements PipelineStep {

    private final GoldAnalytics analytics;

    public GoldStep(GoldAnalytics analytics) {
        this.analytics = analytics;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.GOLD;
    }

    @Override
    public String description() {
        return analytics.description();
    }

    @Override
    public StepResult execute(PipelineContext context) {
        GoldTables gold = analytics.analyze(context.validation());
        context.gold(gold);
        long rows = gold.primaryTable().count();
        return StepResult.completed(rows, analytics.detail(rows));
    }
}
