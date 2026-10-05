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
 * Validador de calidad para datos meteorológicos del SENAMHI.
 */
public final class SenamhiValidator implements TableValidator {

    @Override
    public String table() {
        return OfficialSchema.SENAMHI;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> frame, ReferenceData references) {
        List<Column> rules = List.of(
                ValidationRules.rule(ValidationRules.invalidDate(col("FECHA")), QualityRule.FECHA_INVALIDA),
                ValidationRules.rule(ValidationRules.invalidNonNegative(col("PRECIPITACION_MM")), QualityRule.PRECIPITACION_INVALIDA),
                ValidationRules.rule(ValidationRules.invalidUbigeo(col("UBIGEO")), QualityRule.UBIGEO_INVALIDO),
                ValidationRules.rule(col("DEPARTAMENTO").isNotNull().and(not(upper(trim(col("DEPARTAMENTO"))).isin(
                        ReferenceData.DEPARTAMENTOS_OFICIALES.toArray(Object[]::new)))), QualityRule.DEPARTAMENTO_INEXISTENTE)
        );
        return ValidationRules.check(OfficialSchema.SENAMHI, frame, rules);
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked)
                .select(
                        col("ID").alias("evento_id"),
                        lit("SENAMHI").alias("fuente"),
                        GeoTemporalNormalizer.normalizeDate(col("FECHA")).alias("fecha"),
                        col("ESTACION").alias("estacion"),
                        upper(trim(col("DEPARTAMENTO"))).alias("departamento"),
                        upper(trim(col("PROVINCIA"))).alias("provincia"),
                        upper(trim(col("DISTRITO"))).alias("distrito"),
                        col("UBIGEO").alias("ubigeo"),
                        col("PRECIPITACION_MM").cast("double").alias("precipitacion_mm"),
                        col("TEMP_MAX").cast("double").alias("temp_max"),
                        col("TEMP_MIN").cast("double").alias("temp_min"),
                        coalesce(col("NIVEL_ALERTA"), lit("VERDE")).alias("nivel_alerta"),
                        lit("LLUVIA_METEOROLOGICA").alias("tipo_evento")
                );
    }
}
