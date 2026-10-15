package dev.learning.kafka.own.processing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Формат строки: партиция:оффсет:причина:значение-в-Base64.
 * Причина читается без декодирования — по ней ищут grep'ом.
 * Base64 не содержит двоеточий, поэтому значение — всегда последнее поле.
 */
class PoisonLogTest {

    @TempDir
    Path dir;

    private Path file() {
        return dir.resolve("poison.log");
    }

    @Test
    @DisplayName("одна битая запись — одна строка: партиция, оффсет, читаемая причина, значение в Base64")
    void lineFormat() throws Exception {
        try (PoisonLog log = new PoisonLog(dir)) {
            log.open();
            log.handle(3, 42, "не JSON", "это не json");
        }

        List<String> lines = Files.readAllLines(file());
        assertThat(lines).hasSize(1);
        String line = lines.get(0);
        assertThat(line).startsWith("3:42:");
        assertThat(line).as("причина читается без декодирования").contains("не JSON");
        String encoded = line.substring(line.lastIndexOf(':') + 1);
        assertThat(new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8)).isEqualTo("это не json");
    }

    @Test
    @DisplayName("запись видна в файле сразу, без закрытия — переживёт kill -9")
    void flushedImmediately() throws Exception {
        PoisonLog log = new PoisonLog(dir);
        log.open();

        log.handle(0, 1, "причина", "x");

        assertThat(Files.readAllLines(file())).hasSize(1);
        log.close();
    }

    @Test
    @DisplayName("несколько записей — по строке на каждую")
    void severalRecords() throws Exception {
        try (PoisonLog log = new PoisonLog(dir)) {
            log.open();
            log.handle(0, 1, "a", "x");
            log.handle(1, 2, "b", "y");
            log.handle(2, 3, "c", "z");
        }

        assertThat(Files.readAllLines(file())).hasSize(3);
    }

    @Test
    @DisplayName("перевод строки в причине не разрывает строку файла")
    void newlineInReason() throws Exception {
        try (PoisonLog log = new PoisonLog(dir)) {
            log.open();
            // сообщения Jackson многострочные: текст ошибки + строка с позицией
            log.handle(0, 7, "Unrecognized token\n at [Source: line 1, column 5]", "x");
        }

        assertThat(Files.readAllLines(file())).hasSize(1).first().asString().startsWith("0:7:");
    }

    @Test
    @DisplayName("значение null — запись без исключения")
    void nullValue() throws Exception {
        try (PoisonLog log = new PoisonLog(dir)) {
            log.open();
            assertThatCode(() -> log.handle(0, 9, "пустое значение", null)).doesNotThrowAnyException();
        }

        assertThat(Files.readAllLines(file())).hasSize(1).first().asString().startsWith("0:9:");
    }

    @Test
    @DisplayName("повторное открытие дописывает, а не затирает")
    void appends() throws Exception {
        try (PoisonLog log = new PoisonLog(dir)) {
            log.open();
            log.handle(0, 1, "a", "x");
        }
        try (PoisonLog log = new PoisonLog(dir)) {
            log.open();
            log.handle(0, 2, "b", "y");
        }

        assertThat(Files.readAllLines(file())).hasSize(2);
    }
}