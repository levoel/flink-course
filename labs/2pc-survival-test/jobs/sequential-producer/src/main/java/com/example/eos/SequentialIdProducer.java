package com.example.eos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;

import java.util.Properties;

/**
 * Sequential ID producer для EOS chaos testing.
 *
 * Source эмитит непрерывную последовательность Long ID начиная с 1.
 * State (last emitted ID) check pointing обеспечивает что после recovery
 * source продолжит ровно с того места где остановился — без gaps и
 * без duplicates.
 *
 * Sink: KafkaSink с DeliveryGuarantee.EXACTLY_ONCE. На каждом checkpoint
 * подаёт preCommit (kafka transaction send) → ждёт notifyCheckpointComplete
 * → commitTransaction.
 */
public class SequentialIdProducer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        long transactionTimeoutMs = 900_000;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--transaction-timeout-ms".equals(args[i])) {
                transactionTimeoutMs = Long.parseLong(args[i + 1]);
            }
        }

        DataStream<Long> ids = env.addSource(new SequentialSource())
                .name("SequentialSource")
                .uid("sequential-source");

        Properties producerProps = new Properties();
        producerProps.setProperty("transaction.timeout.ms", String.valueOf(transactionTimeoutMs));

        KafkaSink<Long> sink = KafkaSink.<Long>builder()
                .setBootstrapServers("kafka:9092")
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("producer-output")
                        .setValueSerializationSchema(new LongToStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink-eos-2pc-test")
                .build();

        ids.sinkTo(sink).name("KafkaSink").uid("kafka-sink");

        env.execute("Sequential ID Producer (EOS)");
    }

    /** Source: emit sequential IDs, checkpointable. */
    public static class SequentialSource extends RichSourceFunction<Long>
            implements CheckpointedFunction {

        private volatile boolean running = true;
        private long current = 0L;
        private transient ListState<Long> state;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    current++;
                    ctx.collect(current);
                }
                // Throttle ~1k/sec для лёгкого observability
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            state = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("current-id", Long.class));
            if (ctx.isRestored()) {
                for (Long v : state.get()) {
                    current = Math.max(current, v);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            state.clear();
            state.add(current);
        }
    }

    public static class LongToStringSchema implements org.apache.flink.api.common.serialization.SerializationSchema<Long> {
        @Override
        public byte[] serialize(Long element) {
            return element.toString().getBytes();
        }
    }
}
