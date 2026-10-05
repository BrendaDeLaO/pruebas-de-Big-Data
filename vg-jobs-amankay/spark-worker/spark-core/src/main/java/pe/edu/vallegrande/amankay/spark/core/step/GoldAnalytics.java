package pe.edu.vallegrande.amankay.spark.core.step;

import pe.edu.vallegrande.amankay.spark.core.layer.GoldTables;
import pe.edu.vallegrande.amankay.spark.core.layer.ValidationResult;

/**
 * Interfaz para la lógica de agregación y analítica de datos de la capa Gold.
 */
public interface GoldAnalytics {

    String description();

    GoldTables analyze(ValidationResult silver);

    String detail(long rows);
}
