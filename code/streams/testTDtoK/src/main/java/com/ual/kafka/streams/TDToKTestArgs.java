package com.ual.kafka.streams;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@Parameters(separators="=")
public class TDToKTestArgs implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--kafka-brokers" })
    public String kafkaBrokers = "";

    @Parameter(names = { "--kafka-topic" })
    public String kafkaTopic = "";

    @Parameter(names = { "--include-event-time" }, arity = 1)
    public Boolean includeEventTime = false;

    @Parameter(names = { "--spark-master" })
    public String sparkMaster = "yarn";

    @Parameter(names = { "--teradata-jdbc-options" })
    public String teradataJdbcOptions = "";

    @Parameter(names = { "--teradata-host" })
    public String teradataHost = "";

    @Parameter(names = { "--teradata-database" })
    public String teradataDatabase = "";

    @Parameter(names = { "--teradata-table" })
    public String teradataTable = "";

    @Parameter(names = { "--select-clause" })
    public String selectClause = "*";

    @Parameter(names = { "--where-clause" })
    public String whereClause = "";

    @Parameter(names = { "--user" })
    public String user = "";

    @Parameter(names = { "--password" })
    public String password = "";

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) { throw new RuntimeException(); }
    }
}
