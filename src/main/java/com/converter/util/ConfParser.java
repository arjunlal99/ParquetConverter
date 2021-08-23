package com.converter.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.converter.util.Conf;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import java.io.File;
import java.io.IOException;

public class ConfParser{

    public static Logger logger = LogManager.getLogger(ConfParser.class);

    public Conf conf;
    /*
        path -> absolute path
    */
    public ConfParser(String path){
        try{

            ObjectMapper objectMapper = new ObjectMapper();
            this.conf = objectMapper.readValue(new File(path), Conf.class);

        } catch (JsonMappingException e){

            logger.error("JsonMappingException: Jackson cannot create an instance of the class - this happens if the class is abstract of it is an interface");

        } catch (JsonProcessingException e){

            logger.error("JsonProcessingException");

        } catch (IOException e){

            logger.error("IOException : Failure during reading, writing or searching file");

        }
    }

}
