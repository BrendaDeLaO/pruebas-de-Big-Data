package pe.edu.vallegrande.amankay.spark.official.extraction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import pe.edu.vallegrande.amankay.contracts.OfficialSchema;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;

/**
 * Extractor de datos meteorológicos y de precipitaciones del SENAMHI.
 */
public final class SenamhiExtractor {

    public Dataset<Row> read(SparkSession spark, Path csvFile) {
        StructType schema = new StructType();
        for (String col : OfficialSchema.columns(OfficialSchema.SENAMHI)) {
            schema = schema.add(col, DataTypes.StringType, true);
        }

        Dataset<Row> frame = spark.read()
                .schema(schema)
                .option("header", "true")
                .option("encoding", "UTF-8")
                .option("mode", "PERMISSIVE")
                .csv(csvFile.toString());

        for (String col : OfficialSchema.columns(OfficialSchema.SENAMHI)) {
            frame = frame.withColumn(col, expr("nullif(trim(`" + col + "`), '')"));
        }

        return frame
                .withColumn("_orden", monotonically_increasing_id())
                .withColumn(OfficialExtractor.ROW, row_number().over(Window.orderBy(col("_orden"))).plus(1))
                .drop("_orden")
                .cache();
    }
}
