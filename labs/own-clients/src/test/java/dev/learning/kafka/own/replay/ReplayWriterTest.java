package dev.learning.kafka.own.replay;

import com.fasterxml.jackson.databind.JsonNode;
import dev.learning.kafka.common.JsonSerde;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplayWriterTest {

    @TempDir
    Path dir;

    @Test
    @DisplayName("одна запись — одна строка JSON с координатами, таймстемпом и значением")
    void oneLinePerRecord() throws Exception {
        Path file = dir.resolve("out.jsonl");
        ReplayWriter writer = new ReplayWriter(file);
        writer.open();
        writer.upload(new ReplayWriter.ReplayOrderRecord(3, 1042, 1_792_070_137_081L, "{\"orderId\":\"o-1\"}"));
        writer.upload(new ReplayWriter.ReplayOrderRecord(4, 7, 1_792_070_137_999L, "{\"orderId\":\"o-2\"}"));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(2);
        JsonNode first = JsonSerde.MAPPER.readTree(lines.get(0));
        assertThat(first.get("partition").asInt()).isEqualTo(3);
        assertThat(first.get("offset").asLong()).isEqualTo(1042);
        assertThat(first.get("timestamp").asLong()).isEqualTo(1_792_070_137_081L);
        assertThat(first.get("value").isTextual() ? first.get("value").asText() : first.get("value").toString())
                .contains("o-1");
    }

    @Test
    @DisplayName("значение с кавычками и переводом строки не разрывает строку и сохраняется точно")
    void valueWithSpecialCharacters() throws Exception {
        Path file = dir.resolve("out.jsonl");
        String value = "не json \"в кавычках\"\nвторая строка";
        ReplayWriter writer = new ReplayWriter(file);
        writer.open();
        writer.upload(new ReplayWriter.ReplayOrderRecord(0, 1, 1L, value));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(1);
        assertThat(JsonSerde.MAPPER.readTree(lines.get(0)).get("value").asText()).isEqualTo(value);
    }

    @Test
    @DisplayName("значение null записывается без исключения")
    void nullValue() throws Exception {
        Path file = dir.resolve("out.jsonl");
        ReplayWriter writer = new ReplayWriter(file);
        writer.open();
        writer.upload(new ReplayWriter.ReplayOrderRecord(0, 1, 1L, null));
        writer.close();

        assertThat(Files.readAllLines(file)).hasSize(1);
    }

    @Test
    @DisplayName("open() на существующем файле — ошибка, прошлая выгрузка не затирается")
    void openExistingFails() throws Exception {
        Path file = Files.writeString(dir.resolve("out.jsonl"), "старая выгрузка\n");
        ReplayWriter writer = new ReplayWriter(file);

        assertThatThrownBy(writer::open).isInstanceOf(RuntimeException.class);
        assertThat(Files.readString(file)).isEqualTo("старая выгрузка\n");
    }

    @Test
    @DisplayName("upload() до open() — ошибка")
    void uploadBeforeOpen() {
        ReplayWriter writer = new ReplayWriter(dir.resolve("out.jsonl"));

        assertThatThrownBy(() -> writer.upload(new ReplayWriter.ReplayOrderRecord(0, 1, 1L, "x")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("пустая выгрузка — пустой файл существует")
    void emptyExport() throws Exception {
        Path file = dir.resolve("out.jsonl");
        ReplayWriter writer = new ReplayWriter(file);
        writer.open();
        writer.close();

        assertThat(file).exists();
        assertThat(Files.readAllLines(file)).isEmpty();
    }
}