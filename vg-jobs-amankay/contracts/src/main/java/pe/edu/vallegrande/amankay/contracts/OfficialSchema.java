package pe.edu.vallegrande.amankay.contracts;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Esquema de datos y especificación de metadatos para las fuentes oficiales en Amankay.
 * Define nombres de tablas, columnas de entrada, claves primarias, columnas opcionales y límites.
 */
public final class OfficialSchema {

    public static final String IGP = "igp";
    public static final String SENAMHI = "senamhi";
    public static final String INDECI = "indeci";

    /** Lista de fuentes/tablas oficiales procesadas en el pipeline */
    public static final List<String> TABLES = List.of(IGP, SENAMHI, INDECI);

    /** Columnas canónicas normalizadas comunes a todas las fuentes */
    public static final List<String> COMMON_COLUMNS = List.of("fuente", "fecha", "departamento", "ubigeo", "tipo_evento");

    /** Columnas de entrada originales por cada fuente oficial */
    public static final Map<String, List<String>> COLUMNS = Map.of(
            IGP, List.of("ID", "FECHA_UTC", "HORA_UTC", "LATITUD", "LONGITUD", "PROFUNDIDAD", "MAGNITUD", "FECHA_CORTE"),
            SENAMHI, List.of("ID", "ESTACION", "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "UBIGEO", "FECHA", "PRECIPITACION_MM", "TEMP_MAX", "TEMP_MIN", "NIVEL_ALERTA"),
            INDECI, List.of("ID", "CODIGO_SINPAD", "FECHA_EMERGENCIA", "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "UBIGEO", "TIPO_EVENTO", "DANOS_PERSONAS", "DANOS_VIVIENDAS", "NIVEL_EMERGENCIA")
    );

    /** Claves primarias o de unicidad por fuente */
    public static final Map<String, List<String>> KEYS = Map.of(
            IGP, List.of("ID"),
            SENAMHI, List.of("ID"),
            INDECI, List.of("ID")
    );

    /** Columnas opcionales que pueden venir nulas o vacías */
    public static final Map<String, Set<String>> OPTIONAL_COLUMNS = Map.of(
            IGP, Set.of("FECHA_CORTE"),
            SENAMHI, Set.of("TEMP_MAX", "TEMP_MIN", "NIVEL_ALERTA"),
            INDECI, Set.of("DANOS_PERSONAS", "DANOS_VIVIENDAS")
    );

    /** Descripciones de cada fuente para catálogos y reportes */
    public static final Map<String, String> DESCRIPTIONS = Map.of(
            IGP, "Catálogo Sísmico Nacional del Instituto Geofísico del Perú",
            SENAMHI, "Monitoreo hidrometeorológico y precipitaciones del SENAMHI",
            INDECI, "Registro oficial de emergencias y desastres SINPAD del INDECI"
    );

    public static final int MAX_ROWS_PER_TABLE = 500_000;
    public static final long MAX_BYTES_PER_TABLE = 50L * 1024 * 1024;
    public static final int MAX_CELL_LENGTH = 300;
    public static final String RULES_VERSION = "1.0.0";

    // Límites geográficos de cobertura nacional del Perú (Bounding Box aproximado)
    public static final double MIN_LATITUDE = -19.0;
    public static final double MAX_LATITUDE = 0.5;
    public static final double MIN_LONGITUDE = -82.0;
    public static final double MAX_LONGITUDE = -68.0;

    private OfficialSchema() {
    }

    public static List<String> columns(String table) {
        List<String> columns = COLUMNS.get(table);
        if (columns == null) {
            throw new IllegalArgumentException("Fuente oficial desconocida: " + table);
        }
        return columns;
    }

    public static boolean optional(String table, String column) {
        return OPTIONAL_COLUMNS.getOrDefault(table, Set.of()).contains(column);
    }
}
