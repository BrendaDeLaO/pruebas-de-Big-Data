package pe.edu.vallegrande.amankay.contracts;

/**
 * Reglas de calidad de datos y dimensiones asociadas según estándares DAMA para la plataforma Amankay.
 * Cada regla categoriza el motivo de observación/rechazo hacia la capa de cuarentena.
 */
public enum QualityRule {

    CAMPO_REQUERIDO("COMPLETITUD", "Falta un valor obligatorio en el registro"),

    VALOR_NO_NUMERICO("VALIDEZ", "El valor no es numérico"),

    FECHA_INVALIDA("VALIDEZ", "La fecha no tiene el formato AAAA-MM-DD válido"),

    HORA_INVALIDA("VALIDEZ", "La hora no cumple el formato HH:mm:ss"),

    COORDENADA_INVALIDA("VALIDEZ", "Latitud o longitud fuera del ámbito geográfico nacional"),

    MAGNITUD_FUERA_DE_RANGO("VALIDEZ", "La magnitud sísmica está fuera de la escala válida (0.1 a 10.0)"),

    PROFUNDIDAD_INVALIDA("VALIDEZ", "La profundidad sísmica es negativa o no numérica"),

    PRECIPITACION_INVALIDA("VALIDEZ", "La precipitación en mm es negativa o no numérica"),

    UBIGEO_INVALIDO("VALIDEZ", "El código UBIGEO no tiene el formato oficial de 6 dígitos"),

    NIVEL_ALERTA_INVALIDO("VALIDEZ", "Nivel de alerta no reconocido en el estándar"),

    REGISTRO_DUPLICADO("UNICIDAD", "La clave identificadora del evento ya existe"),

    UBIGEO_INEXISTENTE("INTEGRIDAD", "El UBIGEO no existe en el catálogo nacional"),

    DEPARTAMENTO_INEXISTENTE("INTEGRIDAD", "El departamento no existe en la división política"),

    FECHA_FUTURA("CONSISTENCIA", "La fecha del evento es posterior a la fecha de corte"),

    COORDENADAS_DISCORDANTES("CONSISTENCIA", "Las coordenadas no coinciden con la ubicación declarada");

    private final String dimension;
    private final String description;

    QualityRule(String dimension, String description) {
        this.dimension = dimension;
        this.description = description;
    }

    public String dimension() {
        return dimension;
    }

    public String description() {
        return description;
    }
}
