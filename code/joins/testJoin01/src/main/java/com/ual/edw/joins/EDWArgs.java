package com.ual.edw.joins;


import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@Parameters(separators="=")
public class EDWArgs implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--teradata-jdbc-options" })
    public String teradataJdbcOptions = "";

    @Parameter(names = { "--teradata-host" })
    public String teradataHost = "";

    @Parameter(names = { "--teradata-database" })
    public String teradataDatabase = "";

    @Parameter(names = { "--teradata-user" })
    public String teradataUser = "";

    @Parameter(names = { "--teradata-password" })
    public String teradataPassword = "";

    @Parameter(names = { "--spark-master" })
    public String sparkMaster = "yarn";

    @Parameter(names = { "--source-query" })
    public String sourceQuery = "";

    @Parameter(names = { "--target-query" })
    public String targetQuery = "";

    @Parameter(names = { "--source-join-field" })
    public String sourceJoinField = "";

    @Parameter(names = { "--target-join-field" })
    public String targetJoinField = "";

    @Parameter(names = { "--source-display-num-recs" })
    public Integer sourceDisplayNumRecs = 0;

    @Parameter(names = { "--target-display-num-recs" })
    public Integer targetDisplayNumRecs = 0;

    @Parameter(names = { "--mismatch-display-num-recs" })
    public Integer mismatchDisplayNumRecs = 0;

    @Parameter(names = { "--foj-display-num-recs" })
    public Integer fojDisplayNumRecs = 0;

    @Parameter(names = { "--match-display-num-recs" })
    public Integer matchDisplayNumRecs = 0;

    @Parameter(names = { "--show-explain" }, arity = 1)
    public Boolean showExplain = false;

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) { throw new RuntimeException(); }
    }


}
