package pe.edu.vallegrande.amankay.spark.official.validation;

import java.util.Set;

/**
 * Catálogo maestro de datos geográficos y referencias oficiales del Perú (departamentos y regiones).
 */
public record ReferenceData(Set<String> departamentos, Set<String> ubigeos) {

    public static final Set<String> DEPARTAMENTOS_OFICIALES = Set.of(
            "AMAZONAS", "ANCASH", "APURIMAC", "AREQUIPA", "AYACUCHO",
            "CAJAMARCA", "CALLAO", "CUSCO", "HUANCAVELICA", "HUANUCO",
            "ICA", "JUNIN", "LA LIBERTAD", "LAMBAYEQUE", "LIMA",
            "LORETO", "MADRE DE DIOS", "MOQUEGUA", "PASCO", "PIURA",
            "PUNO", "SAN MARTIN", "TACNA", "TUMBES", "UCAYALI"
    );

    public static ReferenceData standard() {
        return new ReferenceData(DEPARTAMENTOS_OFICIALES, Set.of());
    }

    public static ReferenceData empty() {
        return new ReferenceData(Set.of(), Set.of());
    }

    public boolean isValidDepartamento(String depto) {
        if (depto == null) return false;
        return departamentos.contains(depto.toUpperCase().trim());
    }
}
