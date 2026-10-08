package dev.learning.kafka.own.intake;

import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DeliveryJournal {

    private static final Logger log = LoggerFactory.getLogger(DeliveryJournal.class);
    private static final String ACCEPTED_LOG_FILE_NAME = "accepted.log";
    private static final String ACKED_LOG_FILE_NAME = "acked.log";
    private static final String FAILED_LOG_FILE_NAME = "failed.log";
    private Thread reader;
    private volatile boolean stop = false;
    private final Path accLog;
    private final Path ackLog;
    private final Path failLog;
    private BufferedWriter accW;
    private BufferedWriter ackW;
    private BufferedWriter failW;

    public boolean isHealthy() {
        return reader != null && reader.isAlive();
    }

    public DeliveryJournal(Path folderName) {
        this.accLog = folderName.resolve(ACCEPTED_LOG_FILE_NAME);
        this.ackLog = folderName.resolve(ACKED_LOG_FILE_NAME);
        this.failLog = folderName.resolve(FAILED_LOG_FILE_NAME);
    }

    public void open() throws IOException {
        if (reader == null) {
            this.accW = Files.newBufferedWriter(accLog, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                                                StandardOpenOption.APPEND);
            this.ackW = Files.newBufferedWriter(ackLog, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                                                StandardOpenOption.APPEND);
            this.failW = Files.newBufferedWriter(failLog, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                                                 StandardOpenOption.APPEND);
            reader = new Thread(this::pollJRecords);
            reader.start();
        }
    }

    public void put(JournalEvent event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Поток журнала для записи был прерван", e);
        }
    }

    private void pollJRecords() {
        while (!stop) {
            poll();
        }
        poll();
        log.info("Получен сигнал на остановку чтения событий журнала. В журнале осталось не обработанных событий: {}",
                 queue.size());
    }

    private void poll() {
        List<JournalEvent> evs = new ArrayList<>();
        JournalEvent event;
        try {
            event = queue.poll(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Поток журнала для чтения был прерван", e);
            return;
        }
        if (event == null) {
            return;
        }
        handleEvent(event);
        queue.drainTo(evs);
        if (!evs.isEmpty()) {
            handleEvents(evs);
            evs.clear();
        }
    }

    private void handleEvent(JournalEvent event) {
        handleEvents(List.of(event));
    }

    private void handleEvents(List<JournalEvent> evs) {
        try {
            for (JournalEvent ev : evs) {
                switch (ev) {
                    case JournalEvent.AcceptedEvent acc: {
                        accepted(acc, accW);
                        break;
                    }
                    case JournalEvent.AckedEvent ack: {
                        acked(ack, ackW);
                        break;
                    }
                    case JournalEvent.FailedEvent fail: {
                        failed(fail, failW);
                    }
                }
            }
            accW.flush();
            ackW.flush();
            failW.flush();
        } catch (IOException exc) {
            log.error("Ошибка записи в файл лога", exc);
            throw new RuntimeException(exc);
        }
    }

    private final BlockingQueue<JournalEvent> queue = new LinkedBlockingQueue<>(60_000);

    private void accepted(JournalEvent.AcceptedEvent event, BufferedWriter accW) throws IOException {
        accW.write(event.orderId());
        accW.newLine();
    }

    private void acked(JournalEvent.AckedEvent event, BufferedWriter ackW) throws IOException {
        ackW.write(event.orderId() + ":" + event.partition() + ":" + event.offset());
        ackW.newLine();
    }

    private void failed(JournalEvent.FailedEvent event, BufferedWriter failW) throws IOException {
        String excClass = resolveExcClass(event);
        String message = event.reason() == null ? "" : event.reason().replaceAll("\\R", " ");
        failW.write(event.orderId() + ":" + excClass + ":" + message);
        failW.newLine();
    }

    private String resolveExcClass(JournalEvent.FailedEvent event) {
        if (event.exc() instanceof RetriableException && !event.sendExecuted()) {
            return "отказ в приеме";
        }
        if (event.exc() instanceof RetriableException) {
            return "результат неизвестен";
        } else {
            return "окончательная";
        }
    }

    public void close() {
        stop = true;
        try {
            reader.join(10_000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

//        int idx = 0;
//        while (!queue.isEmpty() && idx < 10) {
//            LockSupport.parkNanos(idx * 1_000_000_000L);
//            idx++;
//        }
        closeQuietly(accW);
        closeQuietly(ackW);
        closeQuietly(failW);
        log.info("Журнал завершил работу. Очередь событий журнала: {}", queue.size());
    }

    private void closeQuietly(BufferedWriter writer) {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        } catch (IOException exc) {
            log.error("Ошибка при закрытии писателя журнала", exc);
        }
    }
}
