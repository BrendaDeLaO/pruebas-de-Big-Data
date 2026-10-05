package pe.edu.vallegrande.amankay.contracts;

/**
 * Protocolo de comunicación desacoplado entre el backend API y los procesos worker Spark.
 * Define códigos de salida del proceso del sistema operativo y nombres estándar de archivos y carpetas.
 */
public final class WorkerProtocol {

    /** Código de salida cuando el lote fue procesado y publicado exitosamente */
    public static final int EXIT_SUCCEEDED = 0;

    /** Código de salida cuando ocurrió un error no recuperable de infraestructura o ejecución */
    public static final int EXIT_FAILED = 1;

    /** Código de salida cuando el lote fue bloqueado por no superar los umbrales de la puerta de calidad */
    public static final int EXIT_QUALITY_FAILED = 2;

    /** Archivo con la traza secuencial de eventos del pipeline */
    public static final String EVENTS_FILE = "events.jsonl";

    /** Archivo con metadatos y configuración de Spark */
    public static final String SPARK_FILE = "spark.json";

    /** Archivo con el resumen final de ejecución del lote */
    public static final String RESULT_FILE = "result.json";

    /** Archivo con el reporte detallado de calidad de datos por tabla y regla */
    public static final String QUALITY_FILE = "quality.json";

    /** Archivo de registro de log del worker */
    public static final String LOG_FILE = "worker.log";

    /** Subdirectorio para tablas Silver */
    public static final String SILVER = "silver";

    /** Subdirectorio para registros observados en cuarentena */
    public static final String QUARANTINE = "quarantine";

    /** Subdirectorio para tablas Gold consolidadas */
    public static final String GOLD = "gold";

    /** Archivo de resumen ejecutivo Gold */
    public static final String SUMMARY_FILE = "gold/resumen.json";

    /** Nombre de la tabla de registros en cuarentena */
    public static final String REJECTED_TABLE = "rechazados";

    private WorkerProtocol() {
    }
}
