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

import com.converter.exceptions.ConfigurationFileNotFoundException;

public class ConfParser{

    public static Logger logger = LogManager.getLogger(ConfParser.class);
    //static variable reference for singleton instance
    private static ConfParser single_instance = null;

    public Conf conf;
    /*
        path -> preferably absolute path
    */
    private ConfParser(String path){
        try{

            if (! new File(path).isFile()) throw new ConfigurationFileNotFoundException();

            ObjectMapper objectMapper = new ObjectMapper();
            this.conf = objectMapper.readValue(new File(path), Conf.class);

        } catch (ConfigurationFileNotFoundException e){

            logger.error("ConfigurationFileNotFoundException : Program cannot find configuration file - ");

        } catch (JsonMappingException e){

            logger.error("JsonMappingException: Jackson cannot create an instance of the class - this happens if the class is abstract of it is an interface");

        } catch (JsonProcessingException e){

            logger.error("JsonProcessingException");

        } catch (IOException e){

            logger.error("IOException : Failure during reading, writing or searching file");

        }
    }

    public static Conf ConfParser(String path){

        if (single_instance == null) single_instance = new ConfParser(path);

        return single_instance.conf;

    }

}
