package com.salman.dhariwala;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;

public class WordCountPoc {
    public static void main(String[] args) {

        // define constants
        String _inputTopic = "word-count-input";
        String _outputTopic = "word-count-output";
        String _bootStrapServer = "localhost:9092";
        String _appId = "word-1-count";

        // define properties
        Properties config =new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,_bootStrapServer);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,_appId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // build topology
        KStreamBuilder builder=new KStreamBuilder();

        builder.stream(Serdes.String(), Serdes.String(), _inputTopic)
                .peek((k,v)-> System.out.println(k+" -- "+v))
                .mapValues(String::toLowerCase)
                .flatMapValues(a->Arrays.asList(a.split(" ")))
                .selectKey((k,v)->v)
                .groupByKey(Serdes.String(), Serdes.String())
                .count("count")
                .to(Serdes.String(),Serdes.Long(),_outputTopic);

        // start stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // add shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
