package com.converter.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.converter.util.Conf;

import java.io.File;
import java.io.IOException;

public class ConfParser{

    public Conf conf;
    /*
        path -> absolute path
    */
    public ConfParser(String path){
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            this.conf = objectMapper.readValue(new File(path), Conf.class);

        } catch (JsonMappingException e){
            e.printStackTrace();

        } catch (JsonProcessingException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }


}
