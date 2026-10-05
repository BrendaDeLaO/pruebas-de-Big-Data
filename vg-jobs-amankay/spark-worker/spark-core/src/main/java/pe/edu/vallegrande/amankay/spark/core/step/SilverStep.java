package pe.edu.vallegrande.amankay.spark.core.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.contracts.WorkerProtocol;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;

import java.io.IOException;
import java.util.Map;

/**
 * Etapa SILVER: Persiste las tablas con datos limpios y tipados en CSV y Parquet.
 */
public final class SilverStep implements PipelineStep {

    @Override
    public PipelineStage stage() {
        return PipelineStage.SILVER;
    }

    @Override
    public String description() {
        return "Los eventos válidos y normalizados se guardan tipados en CSV y Parquet";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        long rows = 0;
        for (Map.Entry<String, Dataset<Row>> table : context.validation().silverTables().entrySet()) {
            rows += context.publisher().publish(WorkerProtocol.SILVER, table.getKey(), table.getValue());
        }
        return StepResult.completed(rows, rows + " eventos limpios en " + context.validation().silverTables().size() + " fuentes");
    }
}
