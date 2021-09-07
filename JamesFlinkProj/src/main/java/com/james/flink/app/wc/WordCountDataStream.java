package com.james.flink.app.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by James on 21-9-7 下午10:37
 */
public class WordCountDataStream {
    private final static Logger logger = LoggerFactory.getLogger(WordCountDataStream.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // dataStream.print();
//        SinkFunction sinkFunction = StreamingFileSink.forRowFormat(
//                new Path("WordCountDataStream.out"), new SimpleStringEncoder<>("UTF-8")).build();

        SinkFunction mySinkFunction = new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                Tuple2<String, Integer> tuple = (Tuple2<String, Integer>) value;
                logger.error(tuple.f0 + ": " + tuple.f1);
                System.out.println(tuple.f0 + ": " + tuple.f1);
            }
        };

        dataStream.addSink(mySinkFunction);

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
