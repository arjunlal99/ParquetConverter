package com.converter.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.converter.util.Field;
public class Conf {

    @JsonProperty("inputFileFormat")
    public String inputFileFormat;
    @JsonProperty("inputDir")
    public String inputDir;
    @JsonProperty("outputDir")
    public String outputDir;
    @JsonProperty("outputFilename")
    public String outputFilename;
    @JsonProperty("fields")
    public Field[] fields;


}
