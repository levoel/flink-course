package com.example.tracer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

/**
 * 4-stage pipeline: Kafka source → keyBy → stateful process → print sink.
 * Каждый stage логирует checkpoint barrier phases для observability ABS protocol.
 */
public class BarrierTracerJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("events")
                .setGroupId("barrier-tracer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> events = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

        events
                .keyBy(line -> line.split(",")[0]) // user_id
                .process(new TracedStatefulProcess())
                .name("StatefulProcess")
                .addSink(new TracedPrintSink())
                .name("TracedPrintSink");

        env.execute("Barrier Tracer Job");
    }

    /** Stateful process с tracking всех checkpoint phases. */
    public static class TracedStatefulProcess
            extends KeyedProcessFunction<String, String, String>
            implements CheckpointedFunction, CheckpointListener {

        private ValueState<Long> counter;
        private transient int subtaskIndex;
        private transient int parallelism;

        @Override
        public void open(OpenContext openContext) {
            this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            this.parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            this.counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("counter", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            Long current = counter.value();
            if (current == null) current = 0L;
            current++;
            counter.update(current);
            out.collect(value + ",count=" + current);
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) {
            // first invocation после initialization
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) {
            long ckptId = ctx.getCheckpointId();
            long t = System.currentTimeMillis();
            log(ckptId, "SNAPSHOT_START", t);
            // simulate work
            try { Thread.sleep(2); } catch (InterruptedException ignored) {}
            log(ckptId, "SNAPSHOT_END", System.currentTimeMillis());
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            log(checkpointId, "COMPLETE_NOTIFY", System.currentTimeMillis());
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            log(checkpointId, "ABORTED", System.currentTimeMillis());
        }

        private void log(long ckpt, String phase, long t) {
            System.out.printf("[barrier-tracer] ckpt=%d op=StatefulProcess task=%d/%d phase=%s t=%d%n",
                    ckpt, subtaskIndex + 1, parallelism, phase, t);
        }
    }

    /** Print sink с tracking checkpoint phases. */
    public static class TracedPrintSink extends RichSinkFunction<String>
            implements CheckpointedFunction, CheckpointListener {

        private transient int subtaskIndex;
        private transient int parallelism;
        private transient long lastValueCount;

        @Override
        public void open(OpenContext openContext) {
            this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            this.parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        }

        @Override
        public void invoke(String value, Context ctx) {
            lastValueCount++;
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) {}

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) {
            long ckptId = ctx.getCheckpointId();
            long t = System.currentTimeMillis();
            log(ckptId, "SNAPSHOT_START", t);
            log(ckptId, "SNAPSHOT_END", System.currentTimeMillis());
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            log(checkpointId, "COMPLETE_NOTIFY", System.currentTimeMillis());
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            log(checkpointId, "ABORTED", System.currentTimeMillis());
        }

        private void log(long ckpt, String phase, long t) {
            System.out.printf("[barrier-tracer] ckpt=%d op=TracedPrintSink task=%d/%d phase=%s t=%d processed=%d%n",
                    ckpt, subtaskIndex + 1, parallelism, phase, t, lastValueCount);
        }
    }
}
