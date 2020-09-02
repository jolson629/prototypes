package com.ual.kafka.streams;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;

@Parameters(separators="=")
public class StreamTestArgs implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--kafka-brokers" })
    public String kafkaBrokers = "";

    @Parameter(names = { "--kafka-topic" })
    public String kafkaTopic = "";

    @Parameter(names = { "--kafka-group-id" })
    public String kafkaGroupId = "";

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) { throw new RuntimeException(); }
    }

}
