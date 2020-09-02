package com.ual.orc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@Parameters(separators="=")
public class WriteOrcArgs {
    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--spark-master" })
    public String sparkMaster = "";

    @Parameter(names = { "--database" })
    public String database = "";

    @Parameter(names = { "--table" })
    public String  table = "";

    @Parameter(names = { "--select" })
    public String  select= "";

    @Parameter(names = { "--date" })
    public String  date = "";

    @Parameter(names = { "--startTime" })
    public String  startTime = "";

    @Parameter(names = { "--endTime" })
    public String  endTime = "";

    @Parameter(names = { "--xml-header" })
    public String  xmlHeader = "";

    @Parameter(names = { "--tag1" })
    public String  tag1 = "";

    @Parameter(names = { "--tag2" })
    public String  tag2 = "";

    @Parameter(names = { "--columnName1" })
    public String  columnName1 = "";

    @Parameter(names = { "--columnName2" })
    public String  columnName2 = "";

    @Parameter(names = { "--limit" })
    public Integer  limit = 0;

    @Parameter(names = { "--targetTables" })
    public String targetTables = "";

    @Parameter(names = { "--subdirs" })
    public String subdirs = "";

    @Parameter(names = { "--rootdir" })
    public String rootDir = "";


    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) { throw new RuntimeException(); }
    }
}
