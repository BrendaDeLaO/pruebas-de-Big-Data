package pe.edu.vallegrande.amankay.contracts;

import java.util.List;

/**
 * Enumeración de tipos de jobs soportados en la plataforma Amankay.
 * Cada tipo define su módulo Maven, clase principal ejecutable, etiqueta descriptiva
 * y conjunto de tablas Gold generadas.
 */
public enum JobType {

    OFFICIAL_BATCH(
            "job-official-batch",
            "pe.edu.vallegrande.amankay.job.official.OfficialBatchJob",
            "Lote de Fuentes Oficiales (IGP, SENAMHI, INDECI)",
            List.of("sismos_resumen", "sismos_diarios", "emergencias_consolidadas")
    ),
    REPORT_STREAM(
            "job-report-stream",
            "pe.edu.vallegrande.amankay.job.report.ReportStreamJob",
            "Streaming de Reportes Ciudadanos",
            List.of("reportes_activos", "senales_zonales")
    ),
    PRIORITY(
            "job-priority",
            "pe.edu.vallegrande.amankay.job.priority.PriorityJob",
            "Motor de Prioridad y Alertas",
            List.of("prioridad_zonas", "alertas_candidatas")
    );

    public static final JobType DEFAULT = OFFICIAL_BATCH;

    private final String module;
    private final String mainClass;
    private final String label;
    private final List<String> goldTables;

    JobType(String module, String mainClass, String label, List<String> goldTables) {
        this.module = module;
        this.mainClass = mainClass;
        this.label = label;
        this.goldTables = goldTables;
    }

    public String module() {
        return module;
    }

    public String jar() {
        return module + ".jar";
    }

    public String mainClass() {
        return mainClass;
    }

    public String label() {
        return label;
    }

    public List<String> goldTables() {
        return goldTables;
    }
}
