package com.converter.mapper;

import com.converter.util.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

import org.apache.parquet.schema.MessageTypeParser;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import java.io.IOException;

public class TsvParser extends Mapper<LongWritable, Text, Void, Group>{

    final String DELIMITER = "\t";
    final String TAB = "\t";
    final String COMMA = ",";

    public static Logger logger = LogManager.getLogger(TsvParser.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        logger.info("TSV Map job started");

        String line = value.toString();
        String[] columns = line.split(DELIMITER);//array of values
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
                    group.append(name, Double.parseDouble(columns[index]));
                    break;
                case("float"):
                    group.append(name, Float.parseFloat(columns[index]));
                    break;
                case("string"):
                    group.append(name, columns[index]);
                    break;
                case("int32"):
                    group.append(name, Integer.parseInt(columns[index]));
                    break;
                case("int64"):
                    group.append(name, Long.parseLong(columns[index]));
                    break;
                case("boolean"):
                    group.append(name, Boolean.parseBoolean(columns[index]));
                    break;
            }
        }

        context.write(null, group);

    }

}
