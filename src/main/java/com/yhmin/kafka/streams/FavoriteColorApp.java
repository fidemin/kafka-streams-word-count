package com.yhmin.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class FavoriteColorApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // for development case
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> favoriteColorInput = builder.stream("favorite-color-input");
        KStream<String, String> userAndColors = favoriteColorInput
                // value should be "{username},{color}" format
                // <null, "jason,blue">, <null,"stephan,red">
                .filter((key, value) -> value.contains(","))

                // <"jason", "jason,blue">, <"stephan","stephan,red">
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())

                // <"jason", "blue">, <"stephan","red">
                .mapValues((value) -> value.split(",")[1].toLowerCase());

        // favorite-color-intermediary cleanup polish should be compact
        userAndColors.to("favorite-color-intermediary", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> userAndColorsTable = builder.table("favorite-color-intermediary");


        KTable<String, Long> favoriteColor = userAndColorsTable
                // group by color within ktable
                .groupBy((key, value) -> new KeyValue<>(value, value))
                .count();

        // favorite-color-output cleanup polish should be compact
        favoriteColor.toStream().to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // only for dev. not for production
        // Delete the application's local state.
        // Note: In real application you'd call `cleanUp()` only under certain conditions.
        // See Confluent Docs for more details:
        // https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#step-2-reset-the-local-environments-of-your-application-instances
        streams.cleanUp();

        streams.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
