package pe.edu.vallegrande.amankay.spark.official.step;

import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.amankay.spark.official.validation.OfficialValidator;

/**
 * Etapa VALIDATION: Ejecuta reglas de completitud, validez y unicidad sobre las fuentes oficiales.
 */
public final class OfficialValidationStep implements PipelineStep {

    private final OfficialValidator validator;

    public OfficialValidationStep(OfficialValidator validator) {
        this.validator = validator;
    }

    @Override
    public PipelineStage stage() {
        return PipelineStage.VALIDATION;
    }

    @Override
    public String description() {
        return "Validación de formatos, coordenadas, magnitudes y detección de duplicados";
    }

    @Override
    public StepResult execute(PipelineContext context) {
        ValidationResult result = validator.validate(context.bronze());
        context.validation(result);
        return StepResult.completed(
                result.received(),
                result.accepted() + " eventos válidos, " + result.rejectedCount() + " observados"
        );
    }
}
