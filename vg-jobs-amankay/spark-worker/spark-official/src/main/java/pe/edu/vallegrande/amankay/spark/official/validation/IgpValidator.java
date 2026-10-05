package pe.edu.vallegrande.amankay.spark.official.validation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.amankay.contracts.OfficialSchema;
import pe.edu.vallegrande.amankay.contracts.QualityRule;
import pe.edu.vallegrande.amankay.spark.official.normalization.GeoTemporalNormalizer;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Validador de calidad para el catálogo sísmico del IGP.
 * Aplica reglas de validez en coordenadas peruanas, magnitud, profundidad y fechas,
 * produciendo la capa Silver normalizada de eventos sísmicos.
 */
public final class IgpValidator implements TableValidator {

    @Override
    public String table() {
        return OfficialSchema.IGP;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> frame, ReferenceData references) {
        List<Column> rules = List.of(
                ValidationRules.rule(ValidationRules.invalidDate(col("FECHA_UTC")), QualityRule.FECHA_INVALIDA),
                ValidationRules.rule(ValidationRules.invalidTime(col("HORA_UTC")), QualityRule.HORA_INVALIDA),
                ValidationRules.rule(ValidationRules.invalidLatitude(col("LATITUD")), QualityRule.COORDENADA_INVALIDA),
                ValidationRules.rule(ValidationRules.invalidLongitude(col("LONGITUD")), QualityRule.COORDENADA_INVALIDA),
                ValidationRules.rule(ValidationRules.invalidMagnitude(col("MAGNITUD")), QualityRule.MAGNITUD_FUERA_DE_RANGO),
                ValidationRules.rule(ValidationRules.invalidNonNegative(col("PROFUNDIDAD")), QualityRule.PROFUNDIDAD_INVALIDA)
        );
        return ValidationRules.check(OfficialSchema.IGP, frame, rules);
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked)
                .select(
                        col("ID").alias("evento_id"),
                        lit("IGP").alias("fuente"),
                        GeoTemporalNormalizer.normalizeDate(col("FECHA_UTC")).alias("fecha"),
                        col("HORA_UTC").alias("hora_utc"),
                        col("LATITUD").cast("double").alias("latitud"),
                        col("LONGITUD").cast("double").alias("longitud"),
                        col("PROFUNDIDAD").cast("double").alias("profundidad_km"),
                        col("MAGNITUD").cast("double").alias("magnitud"),
                        GeoTemporalNormalizer.approximateDepartmentFromCoordinates(col("LATITUD"), col("LONGITUD")).alias("departamento"),
                        lit("SISMO").alias("tipo_evento")
                );
    }
}
