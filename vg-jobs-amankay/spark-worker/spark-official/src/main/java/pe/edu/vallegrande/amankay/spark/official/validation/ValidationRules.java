package pe.edu.vallegrande.amankay.spark.official.validation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import pe.edu.vallegrande.amankay.contracts.OfficialSchema;
import pe.edu.vallegrande.amankay.contracts.QualityRule;
import pe.edu.vallegrande.amankay.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.amankay.spark.official.extraction.OfficialExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Motor de expresiones Spark SQL para validación de reglas de calidad y cuarentena.
 */
public final class ValidationRules {

    public static final String REASONS = ValidationResult.REASONS;

    private ValidationRules() {
    }

    public static Column rule(Column condition, QualityRule rule) {
        return when(coalesce(condition, lit(false)), lit(rule.name()));
    }

    public static Column invalidDate(Column dateCol) {
        return dateCol.isNotNull().and(
                to_date(dateCol, "yyyy-MM-dd").isNull()
                        .and(to_date(dateCol, "yyyy/MM/dd").isNull())
        );
    }

    public static Column invalidTime(Column timeCol) {
        return timeCol.isNotNull().and(
                not(timeCol.rlike("^([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$"))
        );
    }

    public static Column invalidLatitude(Column latCol) {
        Column numericLat = latCol.cast("double");
        return latCol.isNotNull().and(
                numericLat.isNull()
                        .or(numericLat.lt(OfficialSchema.MIN_LATITUDE))
                        .or(numericLat.gt(OfficialSchema.MAX_LATITUDE))
        );
    }

    public static Column invalidLongitude(Column lonCol) {
        Column numericLon = lonCol.cast("double");
        return lonCol.isNotNull().and(
                numericLon.isNull()
                        .or(numericLon.lt(OfficialSchema.MIN_LONGITUDE))
                        .or(numericLon.gt(OfficialSchema.MAX_LONGITUDE))
        );
    }

    public static Column invalidMagnitude(Column magCol) {
        Column numericMag = magCol.cast("double");
        return magCol.isNotNull().and(
                numericMag.isNull().or(numericMag.lt(0.1)).or(numericMag.gt(10.0))
        );
    }

    public static Column invalidNonNegative(Column numCol) {
        Column numericVal = numCol.cast("double");
        return numCol.isNotNull().and(
                numericVal.isNull().or(numericVal.lt(0.0))
        );
    }

    public static Column invalidUbigeo(Column ubigeoCol) {
        return ubigeoCol.isNotNull().and(
                not(ubigeoCol.rlike("^[0-9]{6}$"))
        );
    }

    public static Dataset<Row> check(String table, Dataset<Row> frame, List<Column> rules) {
        List<Column> all = new ArrayList<>();
        all.add(rule(requiredMissing(table), QualityRule.CAMPO_REQUERIDO));
        all.addAll(rules);
        Dataset<Row> withReasons = frame.withColumn(REASONS, concat_ws("|", all.toArray(Column[]::new)));
        return markDuplicates(table, withReasons).cache();
    }

    public static Dataset<Row> valid(Dataset<Row> checked) {
        return checked.filter(col(REASONS).equalTo(""));
    }

    public static Dataset<Row> rejected(String table, Dataset<Row> checked) {
        Column[] original = columns(OfficialSchema.columns(table));
        Column[] key = OfficialSchema.KEYS.get(table).stream()
                .map(column -> coalesce(col(column), lit("?")))
                .toArray(Column[]::new);

        return checked.filter(col(REASONS).notEqual(""))
                .select(
                        lit(table).alias("fuente"),
                        col(OfficialExtractor.ROW),
                        concat_ws(" · ", key).alias("clave"),
                        col(REASONS),
                        to_json(struct(original), Map.of("ignoreNullFields", "false")).alias("registro")
                );
    }

    static Column[] columns(List<String> names) {
        return names.stream().map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
    }

    private static Dataset<Row> markDuplicates(String table, Dataset<Row> frame) {
        List<String> key = OfficialSchema.KEYS.get(table);
        Column keyPresent = lit(true);
        for (String column : key) {
            keyPresent = keyPresent.and(col(column).isNotNull());
        }
        var firstValid = Window.partitionBy(columns(key))
                .orderBy(when(col(REASONS).equalTo(""), 0).otherwise(1), col(OfficialExtractor.ROW));
        return frame
                .withColumn("_rank", row_number().over(firstValid))
                .withColumn(REASONS, concat_ws("|",
                        when(col(REASONS).notEqual(""), col(REASONS)),
                        rule(keyPresent.and(col("_rank").gt(1)), QualityRule.REGISTRO_DUPLICADO)))
                .drop("_rank");
    }

    private static Column requiredMissing(String table) {
        Column missing = lit(false);
        for (String column : OfficialSchema.columns(table)) {
            if (!OfficialSchema.optional(table, column)) {
                missing = missing.or(col(column).isNull());
            }
        }
        return missing;
    }
}
