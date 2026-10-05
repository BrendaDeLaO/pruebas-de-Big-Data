package pe.edu.vallegrande.amankay.spark.official;

import pe.edu.vallegrande.amankay.contracts.OfficialSchema;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.quality.QualityGate;
import pe.edu.vallegrande.amankay.spark.core.step.*;
import pe.edu.vallegrande.amankay.spark.official.extraction.OfficialExtractor;
import pe.edu.vallegrande.amankay.spark.official.step.OfficialBronzeStep;
import pe.edu.vallegrande.amankay.spark.official.step.OfficialValidationStep;
import pe.edu.vallegrande.amankay.spark.official.validation.OfficialValidator;

import java.util.List;

/**
 * Ensamblador de las 7 etapas del pipeline Medallion para fuentes oficiales de Amankay.
 * Flujo: BRONZE -> VALIDATION -> QUARANTINE -> SILVER -> QUALITY_GATE -> GOLD -> EXPORT.
 */
public final class OfficialSteps {

    private OfficialSteps() {
    }

    public static List<PipelineStep> medallion(GoldAnalytics analytics) {
        return List.of(
                new OfficialBronzeStep(new OfficialExtractor()),
                new OfficialValidationStep(new OfficialValidator()),
                new QuarantineStep(),
                new SilverStep(),
                new QualityGateStep(qualityGate()),
                new GoldStep(analytics),
                new ExportStep()
        );
    }

    public static QualityGate qualityGate() {
        return new QualityGate(OfficialSchema.RULES_VERSION, List.of());
    }
}
