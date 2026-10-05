package pe.edu.vallegrande.amankay.contracts;

/**
 * Políticas y umbrales para la asignación de score de prioridad y nivel de urgencia
 * en la plataforma GovTech Amankay.
 */
public final class PriorityPolicy {

    /** Umbral mínimo para clasificar una emergencia o zona en prioridad ALTA */
    public static final double UMBRAL_PRIORIDAD_ALTA = 70.0;

    /** Umbral mínimo para clasificar una emergencia o zona en prioridad MEDIA */
    public static final double UMBRAL_PRIORIDAD_MEDIA = 40.0;

    /** Magnitud sísmica a partir de la cual se considera evento crítico / severo */
    public static final double MAGNITUD_CRITICA = 6.0;

    /** Magnitud sísmica a partir de la cual se considera sismo perceptible / relevante */
    public static final double MAGNITUD_RELEVANTE = 4.5;

    /** Precipitación pluvial en 24h (mm) considerada de alerta naranja/roja */
    public static final double PRECIPITACION_CRITICA_MM = 50.0;

    /** Pesos ponderados para el cálculo explicable del score de prioridad (0 - 100) */
    public static final double PESO_SEVERIDAD_OFICIAL = 0.40;
    public static final double PESO_REPORTES_CIUDADANOS = 0.35;
    public static final double PESO_VULNERABILIDAD_ZONA = 0.25;

    public enum PriorityLevel {
        CRITICA("Alerta Roja - Atención Inmediata"),
        ALTA("Alerta Naranja - Alta Urgencia"),
        MEDIA("Alerta Amarilla - Monitoreo Activo"),
        BAJA("Informativa - Nivel Normal");

        private final String description;

        PriorityLevel(String description) {
            this.description = description;
        }

        public String description() {
            return description;
        }
    }

    private PriorityPolicy() {
    }

    /**
     * Determina el nivel de prioridad según el score numérico calculado (0 a 100).
     */
    public static PriorityLevel classify(double score) {
        if (score >= 85.0) {
            return PriorityLevel.CRITICA;
        }
        if (score >= UMBRAL_PRIORIDAD_ALTA) {
            return PriorityLevel.ALTA;
        }
        if (score >= UMBRAL_PRIORIDAD_MEDIA) {
            return PriorityLevel.MEDIA;
        }
        return PriorityLevel.BAJA;
    }
}
