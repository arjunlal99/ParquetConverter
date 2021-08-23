package com.converter.mapper;
import com.converter.util.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageTypeParser;

//import com.converter.util.LogUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;

public class CsvParser extends Mapper<LongWritable, Text, Void, Group>{
    private static Logger LOGGER = LoggerFactory.getLogger(CsvParser.class);
    final String DELIMITER = ",";
    final String TAB = "\t";
    final String COMMA = ",";

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        LOGGER.info("Map job started");
        String line = value.toString();
        String[] columns = line.split(DELIMITER);//array of values
        for(int i=0;i<columns.length; i++){
            String token = columns[i];
            token = token.trim();
            token = token.replaceAll("^\"|\"$", "");
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
