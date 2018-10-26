package com.salman.dhariwala;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;

public class ColorCountPoc {
    public static void main(String[] args) {

        // define constants
        String _inputTopic = "favourite-colour-input";
        String _outputTopic = "favourite-colour-output";
        String _bootStrapServer = "localhost:9092";
        String _appId = "fav-1-color";

        // define properties
        Properties config =new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,_bootStrapServer);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,_appId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // build topology
        KStreamBuilder builder=new KStreamBuilder();

        builder.stream(Serdes.String(),Serdes.String(),_inputTopic)
                .peek((k,v)->System.out.println("Raw Value: "+k+"---"+v))
                .filter((k,v)->v.contains(","))
                .peek((k,v)->System.out.println("After filter: "+k+"---"+v))
                .map((k,v)-> KeyValue.pair(v.split(",")[0],v.split(",")[1]))
                .peek((k,v)->System.out.println("After mapping: "+k+"---"+v))
                .groupByKey(Serdes.String(),Serdes.String())
                .reduce((iv,cv)->cv)
                .groupBy((k,v)->KeyValue.pair(v,v),Serdes.String(),Serdes.String())
                .count()
                .to(Serdes.String(),Serdes.Long(),_outputTopic);

        // start stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // add shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
