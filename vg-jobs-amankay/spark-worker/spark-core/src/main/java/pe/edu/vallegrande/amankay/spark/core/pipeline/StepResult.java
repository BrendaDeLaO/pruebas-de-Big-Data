package pe.edu.vallegrande.amankay.spark.core.pipeline;

/**
 * Resultado de la ejecución individual de un paso del pipeline.
 */
public record StepResult(long rows, String detail, boolean proceed) {

    public static StepResult completed(long rows, String detail) {
        return new StepResult(rows, detail, true);
    }

    public static StepResult blocked(long rows, String detail) {
        return new StepResult(rows, detail, false);
    }
}
