package com.ual.orc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@Parameters(separators="=")
public class ReadOrcArgs implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    @Parameter(names = { "--orc-path" })
    public String orcPath = "";

    @Parameter(names = { "--spark-master" })
    public String sparkMaster = "";

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) { throw new RuntimeException(); }
    }

}
