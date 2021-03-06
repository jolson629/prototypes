package com.ual.teradata.queries;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@Parameters(separators="=")
public class TeradataArgs
        implements Serializable, Cloneable

{
    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--teradata-host" })
    public String teradataHost = "";

    @Parameter(names = { "--user" })
    public String user = "";

    @Parameter(names = { "--password" })
    public String password = "";

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
