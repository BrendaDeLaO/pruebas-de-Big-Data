package pe.edu.vallegrande.amankay.spark.official.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.amankay.spark.core.layer.BronzeTables;
import pe.edu.vallegrande.amankay.spark.core.layer.ValidationResult;
import pe.edu.vallegrande.amankay.spark.official.extraction.OfficialExtractor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * Validador principal del dominio de fuentes oficiales de Amankay.
 * Ejecuta validadores especializados para IGP, SENAMHI e INDECI,
 * consolidando resultados Silver y registros rechazados.
 */
public final class OfficialValidator {

    private final List<TableValidator> validators;

    public OfficialValidator() {
        this(List.of(new IgpValidator(), new SenamhiValidator(), new IndeciValidator()));
    }

    public OfficialValidator(List<TableValidator> validators) {
        this.validators = List.copyOf(validators);
    }

    public ValidationResult validate(BronzeTables bronze) {
        Map<String, Dataset<Row>> checked = new LinkedHashMap<>();
        Map<String, Dataset<Row>> silver = new LinkedHashMap<>();
        ReferenceData references = ReferenceData.standard();
        Dataset<Row> rejected = null;

        for (TableValidator validator : validators) {
            if (!bronze.tables().containsKey(validator.table())) {
                continue;
            }
            Dataset<Row> table = validator.check(bronze.table(validator.table()), references);
            references = validator.publish(table, references);
            checked.put(validator.table(), table);
            silver.put(validator.table(), validator.silver(table));
            Dataset<Row> invalid = ValidationRules.rejected(validator.table(), table);
            rejected = rejected == null ? invalid : rejected.unionByName(invalid);
        }

        if (rejected == null) {
            throw new IllegalStateException("No se procesó ninguna fuente oficial válida");
        }

        return ValidationResult.of(checked, silver, rejected.orderBy(col("fuente"), col(OfficialExtractor.ROW)));
    }
}
