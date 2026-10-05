package pe.edu.vallegrande.amankay.job.priority;

import pe.edu.vallegrande.amankay.contracts.JobType;
import pe.edu.vallegrande.amankay.contracts.PipelineStage;
import pe.edu.vallegrande.amankay.spark.core.launcher.JobDefinition;
import pe.edu.vallegrande.amankay.spark.core.launcher.JobLauncher;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineStep;
import pe.edu.vallegrande.amankay.spark.core.pipeline.StepResult;

import java.util.List;

/**
 * Job 3: AMANKAY_PRIORITY (Esqueleto inicial v1.0.0)
 * Motor de scoring explicable de prioridades y generación de alertas tempranas
 * combinando fuentes oficiales Silver, reportes ciudadanos y cobertura de asistencia humanitaria.
 */
public final class PriorityJob implements JobDefinition {

    public static void main(String[] args) {
        System.out.println("Iniciando Job 3: AMANKAY_PRIORITY (Esqueleto v1.0.0)...");
        JobLauncher.launch(new PriorityJob(), args);
    }

    @Override
    public JobType type() {
        return JobType.PRIORITY;
    }

    @Override
    public List<PipelineStep> steps() {
        return List.of(
                new PriorityInputStep(),
                new PriorityScoringStep(),
                new PriorityAlertCandidateStep()
        );
    }

    // TODO: Cargar capas Silver consolidadas de fuentes oficiales y reportes ciudadanos
    private static class PriorityInputStep implements PipelineStep {
        @Override
        public PipelineStage stage() {
            return PipelineStage.SILVER;
        }

        @Override
        public String description() {
            return "Consolidación de Silver oficial y señales ciudadanas por UBIGEO (TODO)";
        }

        @Override
        public StepResult execute(pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext context) {
            System.out.println("[TODO] Consolidando datos de entrada multi-fuente...");
            return StepResult.completed(0, "Esqueleto Entrada Prioridad");
        }
    }

    // TODO: Implementar algoritmo ponderado según PriorityPolicy (severidad + reportes + vulnerabilidad)
    private static class PriorityScoringStep implements PipelineStep {
        @Override
        public PipelineStage stage() {
            return PipelineStage.GOLD;
        }

        @Override
        public String description() {
            return "Cálculo del score de urgencia y clasificación territorial (TODO)";
        }

        @Override
        public StepResult execute(pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext context) {
            System.out.println("[TODO] Calculando score de urgencia...");
            return StepResult.completed(0, "Esqueleto Scoring Prioridad");
        }
    }

    // TODO: Generar alert_candidate para flujo de revisión humana antes de pasar a alert_active
    private static class PriorityAlertCandidateStep implements PipelineStep {
        @Override
        public PipelineStage stage() {
            return PipelineStage.EXPORT;
        }

        @Override
        public String description() {
            return "Emisión de candidatos a alerta para validación operativa (TODO)";
        }

        @Override
        public StepResult execute(pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineContext context) {
            System.out.println("[TODO] Generando alertas candidatas...");
            return StepResult.completed(0, "Esqueleto Alertas Candidatas");
        }
    }
}
