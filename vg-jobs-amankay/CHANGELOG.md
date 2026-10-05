# Changelog

Todas las modificaciones notables del proyecto **Amankay - vg-jobs-amankay** serán documentadas en este archivo.

El formato está basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y este proyecto se adhiere a [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-10-04

### Añadido
- **Estructura base multi-módulo Maven**: Configuración raíz con Maven Wrapper (`mvnw`, `mvnw.cmd`) y opciones JVM específicas para Apache Spark 4.x sobre Java 17 (`--add-opens`).
- **Módulo `contracts`**: Modelos puros y contratos desacoplados sin dependencias externas:
  - `JobType`: Catálogo de jobs del ecosistema (`OFFICIAL_BATCH`, `REPORT_STREAM`, `PRIORITY`).
  - `PipelineStage` y `StageStatus`: Etapas Medallion (`BRONZE`, `VALIDATION`, `QUARANTINE`, `SILVER`, `QUALITY_GATE`, `GOLD`, `EXPORT`) y ciclo de vida de ejecución.
  - `StageEvent`: Registro tipado de transiciones para serialización en `events.jsonl`.
  - `QualityRule`: Reglas de calidad DAMA (Completitud, Validez, Unicidad, Integridad, Consistencia) especializadas en emergencias.
  - `WorkerProtocol`: Códigos de salida del proceso (`0` éxito, `1` error, `2` bloqueo por calidad) y nombres canónicos de artefactos.
  - `OfficialSchema`: Esquemas de datos oficiales para IGP, SENAMHI e INDECI con límites geográficos y de cobertura.
  - `PriorityPolicy`: Umbrales de severidad, pesos ponderados y algoritmo de clasificación de urgencia.
- **Módulo `spark-worker/spark-core`**: Motor desacoplado del pipeline:
  - `SparkSessionFactory` y `PermissiveLocalFileSystem`: Inicialización robusta de Spark en modo local y compatibilidad multiplataforma en Windows.
  - `Pipeline` y `PipelineTracker`: Orquestación ordenada con interrupción en caso de bloqueo y trazabilidad en tiempo real.
  - `QualityGate`: Evaluación estricta de tolerancia de rechazo por tabla individual y conservación de registros.
  - `TablePublisher`, `CsvPublisher`, `ParquetPublisher`, `JsonPublisher` y `AtomicFiles`: Persistencia atómica multi-formato sin bloqueos.
  - `JobLauncher`: Manejador de CLI independiente con salida estándar y códigos de retorno al sistema operativo.
- **Módulo `spark-worker/spark-official`**: Dominio de fuentes oficiales del Estado Peruano:
  - Extractores especializados: `IgpExtractor`, `SenamhiExtractor`, `IndeciExtractor` y agregador `OfficialExtractor`.
  - Validadores por fuente: `IgpValidator` (coordenadas nacionales, magnitudes Richter, profundidad), `SenamhiValidator` (precipitaciones pluviales, UBIGEO, departamentos) e `IndeciValidator` (SINPAD, daños).
  - Normalizador geoespacial y temporal: `GeoTemporalNormalizer` (formato ISO `AAAA-MM-DD`, aproximación territorial departamental).
  - Ensamblador Medallion: `OfficialSteps`.
- **Módulo `spark-worker/job-official-batch`**: Job funcional 1 (`AMANKAY_OFFICIAL_BATCH`):
  - Ingesta de catálogo sísmico del IGP.
  - Generación de tablas analíticas Gold: `sismos_resumen` (conteo, promedio y máximos por departamento), `sismos_diarios` (tendencia temporal) y `emergencias_consolidadas`.
  - Pruebas unitarias e integración (`OfficialBatchJobTests`): verificación de publicación exitosa, cuarentena con 5% de errores y bloqueo con tolerancia estricta.
- **Módulos esqueleto**:
  - `spark-worker/job-report-stream`: Esqueleto compilable para ingesta streaming y deduplicación de reportes ciudadanos.
  - `spark-worker/job-priority`: Esqueleto compilable para motor de scoring y alertas tempranas.
  - `backend-api`: Esqueleto Spring Boot 3.5.x WebFlux con endpoints Actuator, configuración reactiva y prueba de contexto.
- **Muestras de datos (`data/`)**: Muestras representativas en CSV de IGP (catálogo sísmico), SENAMHI (meteorología) e INDECI (SINPAD).
