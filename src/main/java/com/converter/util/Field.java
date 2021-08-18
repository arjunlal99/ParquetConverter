package com.converter.util;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Field {
    @JsonProperty("name")
    public String name;
    @JsonProperty("type")
    public String type;
    @JsonProperty("index")
    public String index;
}
