package pe.edu.vallegrande.amankay.spark.official.normalization;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.*;

/**
 * Normalizador geoespacial y temporal para estandarizar formatos de fecha (AAAA-MM-DD),
 * códigos UBIGEO y asignación referencial de departamentos en fuentes oficiales.
 */
public final class GeoTemporalNormalizer {

    private GeoTemporalNormalizer() {
    }

    /**
     * Normaliza cualquier columna de fecha a formato ISO AAAA-MM-DD.
     */
    public static Column normalizeDate(Column dateCol) {
        return coalesce(
                date_format(to_date(dateCol, "yyyy-MM-dd"), "yyyy-MM-dd"),
                date_format(to_date(dateCol, "yyyy/MM/dd"), "yyyy-MM-dd"),
                date_format(to_date(dateCol, "dd/MM/yyyy"), "yyyy-MM-dd")
        );
    }

    /**
     * Asigna el departamento aproximado según rangos de latitud y longitud en territorio peruano
     * cuando la fuente (como el catálogo sísmico IGP) solo provee coordenadas geográficas.
     */
    public static Column approximateDepartmentFromCoordinates(Column latCol, Column lonCol) {
        Column lat = latCol.cast("double");
        Column lon = lonCol.cast("double");

        return when(lat.gt(-4.0).and(lon.lt(-79.5)), lit("TUMBES"))
                .when(lat.gt(-6.0).and(lon.lt(-79.0)), lit("PIURA"))
                .when(lat.gt(-7.2).and(lon.lt(-79.0)), lit("LAMBAYEQUE"))
                .when(lat.gt(-9.0).and(lon.lt(-77.5)), lit("LA LIBERTAD"))
                .when(lat.gt(-10.5).and(lon.lt(-76.5)), lit("ANCASH"))
                .when(lat.gt(-13.5).and(lon.lt(-75.5)), lit("LIMA"))
                .when(lat.gt(-15.5).and(lon.lt(-74.5)), lit("ICA"))
                .when(lat.gt(-16.8).and(lon.lt(-71.0)), lit("AREQUIPA"))
                .when(lat.gt(-17.8).and(lon.lt(-70.5)), lit("MOQUEGUA"))
                .when(lat.gt(-18.5).and(lon.lt(-69.5)), lit("TACNA"))
                .when(lat.gt(-5.0).and(lon.geq(-77.0)), lit("LORETO"))
                .when(lat.gt(-7.5).and(lon.geq(-77.5)).and(lon.lt(-75.5)), lit("SAN MARTIN"))
                .when(lat.gt(-8.0).and(lon.geq(-78.5)).and(lon.lt(-77.0)), lit("CAJAMARCA"))
                .when(lat.gt(-7.0).and(lon.geq(-78.5)).and(lon.lt(-77.5)), lit("AMAZONAS"))
                .when(lat.gt(-10.5).and(lon.geq(-77.0)).and(lon.lt(-74.5)), lit("HUANUCO"))
                .when(lat.gt(-11.5).and(lon.geq(-76.5)).and(lon.lt(-74.0)), lit("PASCO"))
                .when(lat.gt(-12.8).and(lon.geq(-76.0)).and(lon.lt(-73.5)), lit("JUNIN"))
                .when(lat.gt(-14.0).and(lon.geq(-75.5)).and(lon.lt(-74.0)), lit("HUANCAVELICA"))
                .when(lat.gt(-15.5).and(lon.geq(-75.0)).and(lon.lt(-73.0)), lit("AYACUCHO"))
                .when(lat.gt(-15.0).and(lon.geq(-74.0)).and(lon.lt(-72.0)), lit("APURIMAC"))
                .when(lat.gt(-15.5).and(lon.geq(-73.5)).and(lon.lt(-70.0)), lit("CUSCO"))
                .when(lat.gt(-17.5).and(lon.geq(-71.0)).and(lon.lt(-68.5)), lit("PUNO"))
                .when(lat.gt(-13.5).and(lon.geq(-72.0)).and(lon.lt(-68.5)), lit("MADRE DE DIOS"))
                .when(lat.gt(-11.5).and(lon.geq(-75.0)).and(lon.lt(-70.0)), lit("UCAYALI"))
                .otherwise(lit("COSTAS / MAR PERUANO"));
    }
}
