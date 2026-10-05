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
 * Validador de calidad para registros de emergencias del INDECI (SINPAD).
 */
public final class IndeciValidator implements TableValidator {

    @Override
    public String table() {
        return OfficialSchema.INDECI;
    }

    @Override
    public Dataset<Row> check(Dataset<Row> frame, ReferenceData references) {
        List<Column> rules = List.of(
                ValidationRules.rule(ValidationRules.invalidDate(col("FECHA_EMERGENCIA")), QualityRule.FECHA_INVALIDA),
                ValidationRules.rule(ValidationRules.invalidUbigeo(col("UBIGEO")), QualityRule.UBIGEO_INVALIDO),
                ValidationRules.rule(ValidationRules.invalidNonNegative(col("DANOS_PERSONAS")), QualityRule.VALOR_NO_NUMERICO),
                ValidationRules.rule(ValidationRules.invalidNonNegative(col("DANOS_VIVIENDAS")), QualityRule.VALOR_NO_NUMERICO),
                ValidationRules.rule(col("DEPARTAMENTO").isNotNull().and(not(upper(trim(col("DEPARTAMENTO"))).isin(
                        ReferenceData.DEPARTAMENTOS_OFICIALES.toArray(Object[]::new)))), QualityRule.DEPARTAMENTO_INEXISTENTE)
        );
        return ValidationRules.check(OfficialSchema.INDECI, frame, rules);
    }

    @Override
    public Dataset<Row> silver(Dataset<Row> checked) {
        return ValidationRules.valid(checked)
                .select(
                        col("ID").alias("evento_id"),
                        lit("INDECI").alias("fuente"),
                        GeoTemporalNormalizer.normalizeDate(col("FECHA_EMERGENCIA")).alias("fecha"),
                        col("CODIGO_SINPAD").alias("codigo_sinpad"),
                        upper(trim(col("DEPARTAMENTO"))).alias("departamento"),
                        upper(trim(col("PROVINCIA"))).alias("provincia"),
                        upper(trim(col("DISTRITO"))).alias("distrito"),
                        col("UBIGEO").alias("ubigeo"),
                        upper(trim(col("TIPO_EVENTO"))).alias("tipo_evento"),
                        coalesce(col("DANOS_PERSONAS").cast("long"), lit(0L)).alias("danos_personas"),
                        coalesce(col("DANOS_VIVIENDAS").cast("long"), lit(0L)).alias("danos_viviendas"),
                        coalesce(col("NIVEL_EMERGENCIA"), lit("1")).alias("nivel_emergencia")
                );
    }
}
