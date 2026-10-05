package pe.edu.vallegrande.amankay.job.official;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pe.edu.vallegrande.amankay.spark.core.config.SparkSessionFactory;
import pe.edu.vallegrande.amankay.spark.core.config.WorkerConfig;
import pe.edu.vallegrande.amankay.spark.core.pipeline.PipelineResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OfficialBatchJobTests {

    private static SparkSession spark;

    @TempDir
    Path directory;

    @BeforeAll
    static void startSpark() {
        spark = SparkSessionFactory.create(WorkerConfig.load().withoutUi());
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void cleanOfficialDataPublishesGoldAndGeneratesAllArtifacts() throws Exception {
        Path input = Files.createDirectories(directory.resolve("clean-in"));
        Files.writeString(input.resolve("igp.csv"), """
                ID,FECHA_UTC,HORA_UTC,LATITUD,LONGITUD,PROFUNDIDAD,MAGNITUD,FECHA_CORTE
                IGP-01,2026-02-01,04:15:00,-12.043,-77.028,45.0,4.2,2026-02-28
                IGP-02,2026-02-01,08:30:00,-16.398,-71.535,110.0,4.8,2026-02-28
                IGP-03,2026-02-02,11:45:00,-13.531,-71.967,25.0,3.9,2026-02-28
                IGP-04,2026-02-03,14:20:00,-14.067,-75.728,38.0,5.1,2026-02-28
                IGP-05,2026-02-04,01:10:00,-9.527,-77.528,15.0,4.0,2026-02-28
                """);
        Path output = directory.resolve("clean-out");

        PipelineResult result = new OfficialBatchJob().pipeline().run(spark, input, output, 0.05);

        assertTrue(result.published(), "El lote limpio debe ser publicado");
        assertEquals(5, result.received());
        assertEquals(0, result.rejected());
        assertEquals(5, result.accepted());
        assertTrue(result.goldRows() > 0);

        // Validar existencia de archivos de salida del protocolo
        assertTrue(Files.isRegularFile(output.resolve("events.jsonl")), "Debe existir events.jsonl");
        assertTrue(Files.isRegularFile(output.resolve("quality.json")), "Debe existir quality.json");
        assertTrue(Files.isRegularFile(output.resolve("silver/igp.parquet")), "Debe existir silver/igp.parquet");
        assertTrue(Files.isRegularFile(output.resolve("gold/sismos_resumen.csv")), "Debe existir gold/sismos_resumen.csv");
        assertTrue(Files.isRegularFile(output.resolve("gold/sismos_resumen.parquet")), "Debe existir gold/sismos_resumen.parquet");
        assertTrue(Files.isRegularFile(output.resolve("gold/resumen.json")), "Debe existir gold/resumen.json");

        String events = Files.readString(output.resolve("events.jsonl"));
        assertTrue(events.contains("\"stage\":\"EXPORT\",\"status\":\"COMPLETED\""));
    }

    @Test
    void dataWithErrorsIsQuarantinedAndPublishedUnderTolerance() throws Exception {
        Path input = Files.createDirectories(directory.resolve("errors-in"));
        Files.writeString(input.resolve("igp.csv"), """
                ID,FECHA_UTC,HORA_UTC,LATITUD,LONGITUD,PROFUNDIDAD,MAGNITUD,FECHA_CORTE
                IGP-01,2026-02-01,04:15:00,-12.043,-77.028,45.0,4.2,2026-02-28
                IGP-02,2026-02-01,08:30:00,-16.398,-71.535,110.0,4.8,2026-02-28
                IGP-03,2026-02-02,11:45:00,-13.531,-71.967,25.0,3.9,2026-02-28
                IGP-04,2026-02-03,14:20:00,-14.067,-75.728,38.0,5.1,2026-02-28
                IGP-05,2026-02-04,01:10:00,-9.527,-77.528,15.0,4.0,2026-02-28
                IGP-06,2026-02-04,06:55:00,-15.842,-70.021,180.0,4.6,2026-02-28
                IGP-07,2026-02-05,17:05:00,-5.194,-80.632,30.0,4.3,2026-02-28
                IGP-08,2026-02-06,22:18:00,-17.195,-70.935,95.0,4.7,2026-02-28
                IGP-09,2026-02-07,03:40:00,-18.014,-70.252,80.0,5.5,2026-02-28
                IGP-10,2026-02-08,09:12:00,-8.115,-79.029,50.0,4.1,2026-02-28
                IGP-11,2026-02-09,12:33:00,-11.985,-76.850,60.0,4.4,2026-02-28
                IGP-12,2026-02-10,15:50:00,-6.771,-79.840,42.0,3.8,2026-02-28
                IGP-13,2026-02-11,18:22:00,-12.115,-77.150,55.0,6.1,2026-02-28
                IGP-14,2026-02-12,20:05:00,-3.749,-73.253,120.0,4.9,2026-02-28
                IGP-15,2026-02-13,02:44:00,-16.500,-71.400,90.0,4.2,2026-02-28
                IGP-16,2026-02-14,07:18:00,-13.820,-76.250,35.0,5.3,2026-02-28
                IGP-17,2026-02-15,10:30:00,-9.120,-78.500,48.0,4.5,2026-02-28
                IGP-18,2026-02-16,13:15:00,-7.150,-78.510,22.0,3.7,2026-02-28
                IGP-19,2026-02-17,16:40:00,-14.250,-75.500,40.0,4.8,2026-02-28
                IGP-ERR-01,2026-02-18,19:55:00,-11.800,-77.050,52.0,99.9,2026-02-28
                """);
        Path output = directory.resolve("errors-out");

        // 1 error de 20 registros = 5% (0.05 de tolerancia)
        PipelineResult result = new OfficialBatchJob().pipeline().run(spark, input, output, 0.05);

        assertTrue(result.published(), "El lote con 5% de errores debe publicarse con tolerancia 0.05");
        assertEquals(20, result.received());
        assertEquals(1, result.rejected());
        assertEquals(19, result.accepted());

        String quality = Files.readString(output.resolve("quality.json"));
        assertTrue(quality.contains("MAGNITUD_FUERA_DE_RANGO"), "El reporte debe contener la regla MAGNITUD_FUERA_DE_RANGO");

        assertTrue(Files.isRegularFile(output.resolve("quarantine/rechazados.csv")), "Debe existir archivo de cuarentena");
        String quarantine = Files.readString(output.resolve("quarantine/rechazados.csv"));
        assertTrue(quarantine.contains("IGP-ERR-01"), "La fila observada debe estar en cuarentena");
    }

    @Test
    void dataWithErrorsIsBlockedUnderStrictTolerance() throws Exception {
        Path input = Files.createDirectories(directory.resolve("blocked-in"));
        Files.writeString(input.resolve("igp.csv"), """
                ID,FECHA_UTC,HORA_UTC,LATITUD,LONGITUD,PROFUNDIDAD,MAGNITUD,FECHA_CORTE
                IGP-01,2026-02-01,04:15:00,-12.043,-77.028,45.0,4.2,2026-02-28
                IGP-ERR-01,2026-02-18,19:55:00,-11.800,-77.050,52.0,99.9,2026-02-28
                """);
        Path output = directory.resolve("blocked-out");

        // 1 error de 2 registros = 50% de rechazo, con tolerancia 0.01 se bloquea
        PipelineResult result = new OfficialBatchJob().pipeline().run(spark, input, output, 0.01);

        assertFalse(result.published(), "El lote debe ser bloqueado por la puerta de calidad");
        assertFalse(Files.exists(output.resolve("gold/sismos_resumen.csv")), "Gold no debe ser publicado");
        assertTrue(Files.isRegularFile(output.resolve("quarantine/rechazados.csv")), "Debe guardarse la cuarentena");

        String events = Files.readString(output.resolve("events.jsonl"));
        assertTrue(events.contains("\"stage\":\"QUALITY_GATE\",\"status\":\"BLOCKED\""));
    }
}
