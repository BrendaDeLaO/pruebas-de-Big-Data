package pe.edu.vallegrande.amankay.spark.official.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Contrato base para validadores de cada fuente oficial (IGP, SENAMHI, INDECI).
 */
public interface TableValidator {

    String table();

    Dataset<Row> check(Dataset<Row> frame, ReferenceData references);

    Dataset<Row> silver(Dataset<Row> checked);

    default ReferenceData publish(Dataset<Row> checked, ReferenceData references) {
        return references;
    }
}
