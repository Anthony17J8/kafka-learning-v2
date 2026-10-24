package dev.learning.kafka.own.replay;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.learning.kafka.common.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ReplayWriter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplayWriter.class);

    private final Path filePath;
    private BufferedWriter writer;

    public ReplayWriter(Path filePath) {
        this.filePath = filePath;
    }

    public void open() {
        try {
            Files.createFile(filePath);
            writer = Files.newBufferedWriter(filePath);
        } catch (IOException io) {
            throw new IllegalStateException("Ошибка IO при открытии файла " + filePath, io);
        }
    }

    public void upload(ReplayOrderRecord record) {
        if (writer == null) {
            throw new IllegalStateException("Writer не может быть null");
        }
        try {
            String value = JsonSerde.MAPPER.writeValueAsString(record);
            writer.write(value);
            writer.newLine();
        } catch (JsonProcessingException jpe) {
            close();
            throw new IllegalStateException("Ошибка сериализации json", jpe);
        } catch (IOException io) {
            close();
            throw new IllegalStateException("Ошибка записи в файл", io);
        }
    }

    public record ReplayOrderRecord(int partition, long offset, long timestamp, String value) {
    }

    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException io) {
                log.error("Ошибка при закрытии ReplayWriter");
            }
        }
    }
}
