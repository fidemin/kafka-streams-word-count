package com.yhmin.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // when connected, use earliest data
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 1 - fetch stream from kafka: argument -> topic name for input stream
        // result: Kafka Kafka Stream
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // 2 - map values to lower case
        // result: kafka kakfa stream
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())

                // 3 - flatMapValues -> split by space
                // result: <null, "kafka">, <null, "kafka">, <null, "stream">
                .flatMapValues(value -> Arrays.asList(value.split(" ")))

                // 4 - selectKey
                // result: <"kafka", "kafka">, <"kafka", "kafka">, <"stream", "stream">
                .selectKey((key, value) -> value)

                // 5 - group by before count
                // result: (<"kafka", "kafka">, <"kafka", "kafka">), (<"stream", "stream">)
                .groupByKey()

                // 6 - count by key
                // result: <"kafka", 2>, <"stream", 1>
                .count();

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
