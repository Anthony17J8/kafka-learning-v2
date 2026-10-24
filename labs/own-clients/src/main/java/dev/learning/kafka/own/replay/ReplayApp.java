package dev.learning.kafka.own.replay;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ReplayApp {

    private static final Logger log = LoggerFactory.getLogger(ReplayApp.class);

    public static void main(String[] args) {
        ReplayConfig config = new ReplayConfig();
        config.initProps(args);
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config.getConsumerProperties());
             ReplayWriter writer = new ReplayWriter(config.getOut())) {

            registerHook(kafkaConsumer);
            ReplayPlanner planner = new ReplayPlanner(config.getTopic(),
                                                      config.getFrom(),
                                                      kafkaConsumer);
            List<PartitionPlan> plans = planner.buildPlans();
            printPlans(plans);
            if (plans.isEmpty()) {
                throw new RuntimeException("Записи для чтения не найдены");
            } else {
                writer.open();
                BoundedReader reader = new BoundedReader(writer,
                                                         kafkaConsumer,
                                                         plans,
                                                         config.getMaxWaitMs(),
                                                         config.getFrom().toEpochMilli(),
                                                         config.getTo().toEpochMilli());
                boolean isReadSuccess = reader.read();
                printAfterRead(reader);
                if (!isReadSuccess) {
                    throw new RuntimeException("Сообщения не дочитаны");
                }
            }
        }
    }

    private static void printAfterRead(BoundedReader reader) {
        BoundedReader.ReadResult result = reader.getResult();
        log.info("Записей прочитано:");
        log.info("=".repeat(15));
        printResult(result.read());

        log.info("Записей выгружено:");
        log.info("=".repeat(15));
        printResult(result.uploaded());
    }

    private static void printResult(Map<TopicPartition, Long> result) {
        result.forEach((tp, counter) ->
                               log.info("Partition={} - {}", tp.partition(), counter));
    }

    private static void printPlans(List<PartitionPlan> plans) {
        log.info("Составленные планы для чтения:");
        log.info("=".repeat(15));
        plans.forEach(plan -> {
            if (plan.state() == PartitionPlan.PartitionState.EMPTY) {
                log.info("Partition:{}, state=EMPTY", plan.partition().partition());
            } else {
                log.info("Partition={}, foundStartOffset={}, endOffset={}, beginningOffset={}, state={}",
                         plan.partition().partition(),
                         plan.startInterval().offset(),
                         plan.endOffset(),
                         plan.beginningOffset(),
                         plan.state());
            }
        });
    }

    private static void registerHook(KafkaConsumer<String, String> kafkaConsumer) {
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.wakeup();
            try {
                main.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
