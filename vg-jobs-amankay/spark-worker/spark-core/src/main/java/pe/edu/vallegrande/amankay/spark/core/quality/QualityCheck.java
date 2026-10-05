package pe.edu.vallegrande.amankay.spark.core.quality;

import pe.edu.vallegrande.amankay.spark.core.layer.ValidationResult;

import java.util.List;

/**
 * Interfaz funcional para reglas y verificaciones adicionales de calidad en el Quality Gate.
 */
@FunctionalInterface
public interface QualityCheck {

    List<QualityReport.Check> evaluate(ValidationResult validation);
}
