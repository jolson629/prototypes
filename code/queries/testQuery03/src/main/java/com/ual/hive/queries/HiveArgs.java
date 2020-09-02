package com.ual.hive.queries;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@Parameters(separators="=")
public class HiveArgs implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--spark-master" }, required = false)
    public String sparkMaster = "yarn";

    @Parameter(names = { "--database" })
    public String database = "database";

    @Parameter(names = { "--table" })
    public String table = "table";


    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) { throw new RuntimeException(); }
    }

}
