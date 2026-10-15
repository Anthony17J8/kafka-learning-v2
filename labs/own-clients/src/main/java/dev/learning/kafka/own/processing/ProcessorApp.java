package dev.learning.kafka.own.processing;

public class ProcessorApp {

    public static void main(String[] args) throws Exception {

        ProcessorConfig config = ProcessorConfig.create(args);
        Thread main = Thread.currentThread();
        try (BalanceStore balanceStore = new BalanceStore(config.getDbUrl());
             PoisonLog poisonLog = new PoisonLog(config.getPoisonLogDir())) {
            balanceStore.open();
            poisonLog.open();
            ProcessingLoop loop = new ProcessingLoop(
                    new OffsetTracker(),
                    config.getConsumerProps(),
                    poisonLog,
                    balanceStore,
                    config.getPrintPeriodMs(),
                    config.getProcessTimeoutMs(),
                    config.getEmptyPollLimit());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                loop.wakeup();
                try {
                    main.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
            loop.poll();
        }
    }
}
