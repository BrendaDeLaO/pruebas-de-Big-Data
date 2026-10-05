package pe.edu.vallegrande.amankay.spark.official.extraction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pe.edu.vallegrande.amankay.contracts.OfficialSchema;
import pe.edu.vallegrande.amankay.spark.core.layer.BronzeTables;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Extractor agregador para fuentes oficiales de Amankay.
 * Descubre y carga los archivos CSV presentes en el directorio de entrada (IGP, SENAMHI, INDECI).
 */
public final class OfficialExtractor {

    public static final String ROW = "fila";

    private final IgpExtractor igpExtractor = new IgpExtractor();
    private final SenamhiExtractor senamhiExtractor = new SenamhiExtractor();
    private final IndeciExtractor indeciExtractor = new IndeciExtractor();

    public BronzeTables read(SparkSession spark, Path inputDirectory) {
        Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
        Map<String, Long> rows = new LinkedHashMap<>();

        // IGP es la fuente mandatoria para el lote base oficial
        Path igpFile = resolveTableFile(inputDirectory, OfficialSchema.IGP);
        if (igpFile != null && Files.isRegularFile(igpFile)) {
            Dataset<Row> frame = igpExtractor.read(spark, igpFile);
            tables.put(OfficialSchema.IGP, frame);
            rows.put(OfficialSchema.IGP, frame.count());
        } else {
            throw new IllegalArgumentException("Falta el archivo de catálogo sísmico " + OfficialSchema.IGP + ".csv en " + inputDirectory);
        }

        // SENAMHI opcional u obligatorio si existe en el lote
        Path senamhiFile = resolveTableFile(inputDirectory, OfficialSchema.SENAMHI);
        if (senamhiFile != null && Files.isRegularFile(senamhiFile)) {
            Dataset<Row> frame = senamhiExtractor.read(spark, senamhiFile);
            tables.put(OfficialSchema.SENAMHI, frame);
            rows.put(OfficialSchema.SENAMHI, frame.count());
        }

        // INDECI opcional u obligatorio si existe en el lote
        Path indeciFile = resolveTableFile(inputDirectory, OfficialSchema.INDECI);
        if (indeciFile != null && Files.isRegularFile(indeciFile)) {
            Dataset<Row> frame = indeciExtractor.read(spark, indeciFile);
            tables.put(OfficialSchema.INDECI, frame);
            rows.put(OfficialSchema.INDECI, frame.count());
        }

        return new BronzeTables(tables, rows);
    }

    private Path resolveTableFile(Path baseDir, String tableName) {
        Path direct = baseDir.resolve(tableName + ".csv");
        if (Files.isRegularFile(direct)) {
            return direct;
        }
        Path nested = baseDir.resolve(tableName).resolve(tableName + ".csv");
        if (Files.isRegularFile(nested)) {
            return nested;
        }
        return null;
    }
}
