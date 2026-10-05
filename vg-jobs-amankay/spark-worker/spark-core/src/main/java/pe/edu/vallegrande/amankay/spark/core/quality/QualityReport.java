package pe.edu.vallegrande.amankay.spark.core.quality;

import java.util.List;
import java.util.Map;

/**
 * Reporte detallado y estructurado de la evaluación de calidad de datos generada por el Quality Gate.
 * Se serializa en quality.json en el directorio de salida del job.
 */
public record QualityReport(
        String rulesVersion,
        long received,
        long accepted,
        long rejected,
        double rejectedRatio,
        double maxRejectedRatio,
        boolean published,
        String decision,
        List<TableQuality> tables,
        Map<String, Long> dimensions,
        List<ReasonQuality> reasons,
        List<Check> checks
) {

    public record TableQuality(String table, long received, long accepted, long rejected, double rejectedRatio, boolean withinLimit) {
    }

    public record ReasonQuality(String table, String rule, String dimension, String description, long rows) {
    }

    public record Check(String name, boolean passed, String detail) {
    }
}
