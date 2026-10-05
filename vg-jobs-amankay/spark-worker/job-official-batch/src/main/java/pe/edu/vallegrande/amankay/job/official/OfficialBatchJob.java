package pe.edu.vallegrande.amankay.job.official;

import pe.edu.vallegrande.amankay.contracts.JobType;
import pe.edu.vallegrande.amankay.job.official.analytics.OfficialBatchAnalytics;
import pe.edu.vallegrande.amankay.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.amankay.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.official.OfficialSteps;

import java.util.List;

/**
 * Job 1: AMANKAY_OFFICIAL_BATCH
 * Procesa por lotes las fuentes oficiales de emergencias (IGP, SENAMHI, INDECI) a través
 * del pipeline Medallion, aplicando validación, cuarentena, normalización y analítica territorial Gold.
 */
public final class OfficialBatchJob implements JobDefinition {

    public static void main(String[] args) {
        JobLauncher.launch(new OfficialBatchJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.OFFICIAL_BATCH;
    }

    @Override
    public List<PipelineStep> steps() {
        return OfficialSteps.medallion(new OfficialBatchAnalytics());
    }
}
