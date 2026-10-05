package pe.edu.vallegrande.amankay.spark.core.publication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Publicador JSON y JSON Lines para eventos, reportes de calidad y resúmenes ejecutivos.
 */
public final class JsonPublisher {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper COMPACT = new ObjectMapper();

    private JsonPublisher() {
    }

    public static void write(Path target, Object value) throws IOException {
        Path temporary = AtomicFiles.temporaryFor(target);
        Files.write(temporary, MAPPER.writeValueAsBytes(value));
        AtomicFiles.publish(temporary, target);
    }

    public static void appendLine(Path target, Object value) throws IOException {
        Files.createDirectories(target.getParent());
        Files.writeString(
                target,
                COMPACT.writeValueAsString(value) + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }
}
