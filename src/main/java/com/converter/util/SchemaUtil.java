package com.converter.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.converter.util.Field;
import com.converter.util.Conf;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import java.io.File;
import java.io.IOException;

public class SchemaUtil {

    public static Logger logger = LogManager.getLogger(SchemaUtil.class);

    /*
        Function to generate schema using Configuration object passed from main function
    */
    public static String generateSchema(String path) {

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            Conf jsonConf = objectMapper.readValue(new File(path), Conf.class);
            String schema = "message schema{\n";
            for (Field field : jsonConf.fields) {
                schema = schema + "required " + (field.type.equals("string") ? "binary" : field.type) + " " + field.name + ";\n";
            }
            schema = schema + "}";
            return schema;
        } catch (JsonMappingException e) {
            logger.error("JsonMappingException: Jackson cannot create an instance of the class - this happens if the class is abstract of it is an interface");
            return null;
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException");
            return null;
        } catch (IOException e) {
            logger.error("IOException : Failure during reading, writing or searching file");
            return null;
        }
    }

    public static String fieldsToString(Field[] fields){
        String fieldsString = "";
        String COMMA =",", TAB = "\t";
        for (Field field: fields){
            fieldsString = fieldsString + field.name + COMMA + field.type + COMMA + field.index + TAB;
        }
        return fieldsString;
    }
}

