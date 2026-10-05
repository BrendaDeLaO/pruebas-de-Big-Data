package pe.edu.vallegrande.amankay.spark.core.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.contracts.WorkerProtocol;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;
import pe.edu.vallegrande.amankay.spark.core.publication.JsonPublisher;

import java.io.IOException;
import java.util.Map;

/**
 * Etapa EXPORT: Publica las tablas analíticas Gold en CSV, Parquet y el resumen ejecutivo en JSON.
 */
public final class ExportStep implements PipelineStep {

    @Override
    public PipelineStage stage() {
        return PipelineStage.EXPORT;
    }

    @Override
    public String description() {
        return "Publicación de tablas Gold en formatos CSV, Parquet y resumen ejecutivo JSON";
    }

    @Override
    public StepResult execute(PipelineContext context) throws IOException {
        long files = 0;
        for (Map.Entry<String, Dataset<Row>> table : context.gold().tables().entrySet()) {
            context.publisher().publish(WorkerProtocol.GOLD, table.getKey(), table.getValue());
            files += context.publisher().formatsPerTable();
        }
        JsonPublisher.write(context.output().resolve(WorkerProtocol.SUMMARY_FILE), context.gold().summaryValues());
        return StepResult.completed(files + 1, (files + 1) + " archivos publicados en capa Gold");
    }
}
