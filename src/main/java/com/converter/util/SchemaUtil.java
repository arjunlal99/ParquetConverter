package com.converter.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.converter.util.Field;
import com.converter.util.Conf;

import java.io.File;
import java.io.IOException;

public class SchemaUtil {

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
            e.printStackTrace();
            return null;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
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

