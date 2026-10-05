package pe.edu.vallegrande.amankay.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Aplicación principal del Backend API reactivo de la plataforma Amankay.
 * Provee servicios REST reactivos con Spring Boot WebFlux para consulta de alertas,
 * mapas de calor de riesgo y orquestación de lotes de datos.
 */
@SpringBootApplication
public class AmankayApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(AmankayApiApplication.class, args);
    }
}
