package com.example.wh;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Write-heavy job для RocksDB compaction observability.
 *
 * Каждый event прокидывается через keyBy(key % 10M), и stateful operator
 * увеличивает counter в ValueState. Это создаёт высокое write давление
 * на RocksDB → forced compactions → observable LSM tree dynamics.
 */
public class WriteHeavyCounter {

    private static final int KEY_SPACE = 10_000_000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // NumberSequenceSource не подходит — он завершается. Делаем infinite generator.
        DataStream<Long> events = env.addSource(new InfiniteRandomKeySource(), "random-keys");

        events
                .keyBy(k -> k % KEY_SPACE)
                .process(new IncrementCounter())
                .name("IncrementCounter")
                .addSink(new DiscardingSink());

        env.execute("WriteHeavyCounter");
    }

    public static class IncrementCounter
            extends KeyedProcessFunction<Long, Long, Long> {

        private transient ValueState<Long> counter;

        @Override
        public void open(OpenContext ctx) {
            counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("counter", Long.class));
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
            Long current = counter.value();
            if (current == null) current = 0L;
            current++;
            counter.update(current);
            // Без эмита downstream, sink — discarding
            if (current % 1000 == 0) {
                out.collect(current);
            }
        }
    }

    public static class InfiniteRandomKeySource
            implements org.apache.flink.streaming.api.functions.source.SourceFunction<Long> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) {
            ThreadLocalRandom r = ThreadLocalRandom.current();
            while (running) {
                ctx.collect((long) r.nextInt(KEY_SPACE));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class DiscardingSink implements
            org.apache.flink.streaming.api.functions.sink.SinkFunction<Long> {
        @Override
        public void invoke(Long value, Context context) {
            // discard
        }
    }
}
