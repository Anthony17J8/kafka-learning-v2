package dev.learning.kafka.own.processing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Optional;

public class PoisonLog implements AutoCloseable {
    private static final String LOG_PATTERN = "%d:%d:%s:%s";
    private static final String POISON_LOG_FILE_NAME = "poison.log";

    private final Path filePath;
    private BufferedWriter writer;

    public PoisonLog(Path folderName) {
        this.filePath = folderName.resolve(Path.of(POISON_LOG_FILE_NAME));
    }

    public void open() throws Exception {
        this.writer = Files.newBufferedWriter(filePath,
                                              StandardOpenOption.CREATE,
                                              StandardOpenOption.WRITE,
                                              StandardOpenOption.APPEND);
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

    public void handle(int partition, long offset, String reason, String value) {
        String nonNReason = Optional.ofNullable(reason).orElse("").replaceAll("\\R", " ");
        String nonNValue = Optional.ofNullable(value).orElse("");
        String base64 = "";
        if (!nonNValue.isEmpty()) {
            base64 = Base64.getEncoder().encodeToString(nonNValue.getBytes(StandardCharsets.UTF_8));
        }


        try {
            writer.write(LOG_PATTERN.formatted(partition, offset, nonNReason, base64));
            writer.newLine();
            writer.flush();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
