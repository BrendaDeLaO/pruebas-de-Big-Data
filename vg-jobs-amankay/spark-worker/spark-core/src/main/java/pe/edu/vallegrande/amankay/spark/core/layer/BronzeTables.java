package pe.edu.vallegrande.amankay.spark.core.layer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Contenedor in-memory de DataFrames correspondientes a la capa Bronze (datos crudos ingestados).
 */
public record BronzeTables(Map<String, Dataset<Row>> tables, Map<String, Long> rows) {

    public Dataset<Row> table(String name) {
        Dataset<Row> table = tables.get(name);
        if (table == null) {
            throw new IllegalArgumentException("Tabla Bronze no encontrada: " + name);
        }
        return table;
    }

    public long totalRows() {
        return rows.values().stream().mapToLong(Long::longValue).sum();
    }

    public String describe() {
        return rows.entrySet().stream()
                .map(entry -> entry.getKey() + " (" + entry.getValue() + " filas)")
                .collect(Collectors.joining(", "));
    }

    public void release() {
        tables.values().forEach(Dataset::unpersist);
    }
}
