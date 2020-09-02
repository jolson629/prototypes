package com.ual.orc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@Parameters(separators="=")

public class WriteHiveOrcArgs implements Serializable, Cloneable{

    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--spark-master" })
    public String sparkMaster = "";

    @Parameter(names = { "--teradata-jdbc-options" })
    public String teradataJdbcOptions = "";

    @Parameter(names = { "--teradata-host" })
    public String teradataHost = "";

    @Parameter(names = { "--user" })
    public String user = "";

    @Parameter(names = { "--password" })
    public String password = "";

    @Parameter(names = { "--sql" })
    public String sql = "sql";

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) { throw new RuntimeException(); }
    }

}
