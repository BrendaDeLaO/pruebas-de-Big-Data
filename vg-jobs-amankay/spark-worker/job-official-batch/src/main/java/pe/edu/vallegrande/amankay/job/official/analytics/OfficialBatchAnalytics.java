package pe.edu.vallegrande.amankay.job.official.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.amankay.contracts.OfficialSchema;
import pe.edu.vallegrande.amankay.contracts.PriorityPolicy;
import pe.edu.vallegrande.amankay.spark.core.layer.GoldTables;
import pe.edu.vallegrande.amankay.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.amankay.spark.core.step.GoldAnalytics;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Motor de analítica de datos para el Job de Fuentes Oficiales de Amankay.
 * Calcula agregaciones territoriales, estadísticas de magnitud sísmica,
 * series temporales diarias y resúmenes ejecutivos para la toma de decisiones.
 */
public final class OfficialBatchAnalytics implements GoldAnalytics {

    public static final String SISMOS_RESUMEN = "sismos_resumen";
    public static final String SISMOS_DIARIOS = "sismos_diarios";
    public static final String EMERGENCIAS_CONSOLIDADAS = "emergencias_consolidadas";

    @Override
    public String description() {
        return "Generación de analítica sísmica y consolidación territorial por departamento en capa Gold";
    }

    @Override
    public GoldTables analyze(ValidationResult silver) {
        Dataset<Row> igp = silver.silver(OfficialSchema.IGP);
        if (igp == null || igp.isEmpty()) {
            throw new IllegalStateException("No se encontraron registros sísmicos de IGP en capa Silver para analítica");
        }

        // 1. Resumen de sismos por departamento
        Dataset<Row> sismosResumen = igp.groupBy("departamento")
                .agg(
                        count("*").alias("total_sismos"),
                        round(max("magnitud"), 1).alias("magnitud_maxima"),
                        round(avg("magnitud"), 2).alias("magnitud_promedio"),
                        round(avg("profundidad_km"), 1).alias("profundidad_promedio_km"),
                        sum(when(col("magnitud").geq(PriorityPolicy.MAGNITUD_RELEVANTE), 1).otherwise(0)).alias("sismos_relevantes"),
                        sum(when(col("magnitud").geq(PriorityPolicy.MAGNITUD_CRITICA), 1).otherwise(0)).alias("sismos_criticos")
                )
                .orderBy(col("total_sismos").desc(), col("magnitud_maxima").desc());

        // 2. Conteo de sismos diarios por fecha
        Dataset<Row> sismosDiarios = igp.groupBy("fecha")
                .agg(
                        count("*").alias("total_sismos"),
                        round(max("magnitud"), 1).alias("magnitud_maxima"),
                        round(min("profundidad_km"), 1).alias("profundidad_minima_km")
                )
                .orderBy(col("fecha").asc());

        // 3. Emergencias consolidadas (IGP + SENAMHI/INDECI si existen)
        Dataset<Row> eventos = igp.select(
                col("evento_id"),
                col("fuente"),
                col("fecha"),
                col("departamento"),
                col("tipo_evento")
        );

        if (silver.silver(OfficialSchema.SENAMHI) != null) {
            Dataset<Row> senamhi = silver.silver(OfficialSchema.SENAMHI)
                    .select(col("evento_id"), col("fuente"), col("fecha"), col("departamento"), col("tipo_evento"));
            eventos = eventos.unionByName(senamhi);
        }

        if (silver.silver(OfficialSchema.INDECI) != null) {
            Dataset<Row> indeci = silver.silver(OfficialSchema.INDECI)
                    .select(col("evento_id"), col("fuente"), col("fecha"), col("departamento"), col("tipo_evento"));
            eventos = eventos.unionByName(indeci);
        }

        Dataset<Row> emergenciasConsolidadas = eventos.groupBy("departamento", "tipo_evento")
                .agg(count("*").alias("total_eventos"))
                .orderBy(col("departamento").asc(), col("total_eventos").desc());

        // 4. Métricas resumen globales
        Dataset<Row> summary = igp.agg(
                count("*").alias("total_sismos"),
                countDistinct("departamento").alias("departamentos_afectados"),
                round(max("magnitud"), 1).alias("magnitud_maxima"),
                sum(when(col("magnitud").geq(PriorityPolicy.MAGNITUD_CRITICA), 1).otherwise(0)).alias("sismos_criticos"),
                sum(when(col("magnitud").geq(PriorityPolicy.MAGNITUD_RELEVANTE), 1).otherwise(0)).alias("sismos_relevantes")
        );

        Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
        tables.put(SISMOS_RESUMEN, sismosResumen);
        tables.put(SISMOS_DIARIOS, sismosDiarios);
        tables.put(EMERGENCIAS_CONSOLIDADAS, emergenciasConsolidadas);

        return new GoldTables(SISMOS_RESUMEN, tables, summary);
    }

    @Override
    public String detail(long rows) {
        return rows + " departamentos analizados en resumen sísmico Gold";
    }
}
