package dev.learning.kafka.own.intake;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryJournalTest {

    @TempDir
    Path dir;

    @Test
    @DisplayName("каждый тип события пишется в свой файл")
    void eachEventTypeGoesToItsFile() throws IOException {
        DeliveryJournal journal = open();

        journal.put(new JournalEvent.AcceptedEvent("o-1"));
        journal.put(new JournalEvent.AckedEvent("o-1", 3, 42L));
        journal.put(new JournalEvent.AcceptedEvent("o-2"));
        journal.put(new JournalEvent.FailedEvent("o-2", new RecordTooLargeException("big"), "big", true));
        journal.close();

        assertThat(lines("accepted.log")).containsExactly("o-1", "o-2");
        assertThat(lines("acked.log")).singleElement().asString().startsWith("o-1").contains("3").contains("42");
        assertThat(lines("failed.log")).singleElement().asString().startsWith("o-2");
    }

    @Test
    @DisplayName("закрытие дописывает всю очередь: ни одно событие не теряется")
    void closeDrainsEverything() throws IOException {
        DeliveryJournal journal = open();

        for (int i = 0; i < 20_000; i++) {
            journal.put(new JournalEvent.AcceptedEvent("o-" + i));
        }
        journal.close();

        assertThat(lines("accepted.log")).hasSize(20_000);
    }

    @Test
    @DisplayName("журнал без событий остаётся работоспособным и не падает")
    void staysHealthyWhenIdle() throws IOException, InterruptedException {
        DeliveryJournal journal = open();

        Thread.sleep(500);
        assertThat(journal.isHealthy()).as("поток журнала жив после простоя").isTrue();

        journal.put(new JournalEvent.AcceptedEvent("after-idle"));
        journal.close();

        assertThat(lines("accepted.log")).containsExactly("after-idle");
    }

    @Test
    @DisplayName("переводы строк в причине не разрывают строку файла")
    void newlinesInReasonAreRemoved() throws IOException {
        DeliveryJournal journal = open();

        journal.put(new JournalEvent.FailedEvent("o-1", new RecordTooLargeException("x"), "a\nb\r\nc", true));
        journal.close();

        assertThat(lines("failed.log")).singleElement().asString().contains("a b c");
    }

    @Test
    @DisplayName("причина null не ломает журнал")
    void nullReasonIsHandled() throws IOException {
        DeliveryJournal journal = open();

        journal.put(new JournalEvent.FailedEvent("o-1", new RecordTooLargeException("x"), null, true));
        journal.put(new JournalEvent.AcceptedEvent("o-2"));
        journal.close();

        assertThat(lines("failed.log")).singleElement().asString().startsWith("o-1");
        assertThat(lines("accepted.log")).containsExactly("o-2");
    }

    @Test
    @DisplayName("классы ошибок: отказ в приёме, результат неизвестен, окончательная")
    void errorClasses() throws IOException {
        DeliveryJournal journal = open();

        journal.put(new JournalEvent.FailedEvent("rejected",
                                                 new BufferExhaustedException("no memory"), "no memory", false));
        journal.put(new JournalEvent.FailedEvent("unknown",
                                                 new TimeoutException("Expiring 1 record(s)"), "Expiring 1 record(s)", true));
        journal.put(new JournalEvent.FailedEvent("too-large",
                                                 new RecordTooLargeException("big"), "big", true));
        journal.put(new JournalEvent.FailedEvent("no-access",
                                                 new TopicAuthorizationException("no access"), "no access", true));
        journal.close();

        List<String> failed = lines("failed.log");
        assertThat(lineFor(failed, "rejected")).contains("отказ в приеме");
        assertThat(lineFor(failed, "unknown")).contains("результат неизвестен");
        assertThat(lineFor(failed, "too-large")).contains("окончательная");
        assertThat(lineFor(failed, "no-access"))
                .as("любая неретраибельная ошибка — окончательная, а не «неизвестная»")
                .contains("окончательная");
    }

    @Test
    @DisplayName("несуществующий каталог — ошибка при открытии, до отправки первого заказа")
    void openFailsForMissingFolder() {
        DeliveryJournal journal = new DeliveryJournal(dir.resolve("no-such-dir"));

        assertThatThrownBy(journal::open).isInstanceOf(IOException.class);
    }

    private DeliveryJournal open() throws IOException {
        DeliveryJournal journal = new DeliveryJournal(dir);
        journal.open();
        return journal;
    }

    private List<String> lines(String file) throws IOException {
        return Files.readAllLines(dir.resolve(file));
    }

    private static String lineFor(List<String> lines, String orderId) {
        return lines.stream().filter(l -> l.startsWith(orderId)).findFirst().orElseThrow();
    }
}