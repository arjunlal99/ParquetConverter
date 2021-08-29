package com.converter.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

import org.apache.parquet.schema.MessageTypeParser;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JsonParser extends Mapper<LongWritable, Text, Void, Group>{

    final String TAB = "\t";
    final String COMMA = ",";

    private static final Logger logger = LogManager.getLogger(JsonParser.class);

    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String json = value.toString();
        json = json.replace("{", "");
        json = json.replace("}", "");
        String [] jsonFields = json.split(",");
        List<String> columns = new ArrayList<String>();
        for (String field : jsonFields){
            columns.add(field.split(":")[1]);
        }

        Configuration conf = context.getConfiguration();
        String schema = conf.get("schema");
        String[] fields = conf.get("fields").split(TAB);    //split fields encoded into string

        GroupFactory factory = new SimpleGroupFactory(MessageTypeParser.parseMessageType(schema));
        Group group = factory.newGroup();

        for (String field: fields){
            String[] fieldString = field.split(COMMA);
            String name = fieldString[0];
            String type = fieldString[1];
            int index = Integer.parseInt(fieldString[2]);

            switch(type){
                case("double"):
                    group.append(name, Double.parseDouble(columns.get(index)));
                    break;
                case("float"):
                    group.append(name, Float.parseFloat(columns.get(index)));
                    break;
                case("string"):
                    group.append(name, columns.get(index));
                    break;
                case("int32"):
                    group.append(name, Integer.parseInt(columns.get(index)));
                    break;
                case("int64"):
                    group.append(name, Long.parseLong(columns.get(index)));
                    break;
                case("boolean"):
                    group.append(name, Boolean.parseBoolean(columns.get(index)));
                    break;
            }
        }
    }

}
