package com.converter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;

//importing individual mapper classes for each file format
import com.converter.mapper.CsvParser;
import com.converter.mapper.TsvParser;
import com.converter.mapper.JsonParser;

import com.converter.util.Conf;
import com.converter.util.ConfParser;
import com.converter.util.SchemaUtil;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

public class ParquetConverter {

    private final static Class TSV = TsvParser.class;
    private final static Class CSV = CsvParser.class;
    private final static Class JSON = JsonParser.class;

    public static Logger logger = LogManager.getLogger(ParquetConverter.class);

    public static void main(String [] args) throws Exception{
        ConfParser confParser = new ConfParser(args[0]);
        Configuration conf = new Configuration();
        String schema = SchemaUtil.generateSchema((args[0]));

        conf.set("fields", SchemaUtil.fieldsToString(confParser.conf.fields));
        conf.set("schema", schema);
        logger.info(schema);

        Job job = Job.getInstance(conf, "ParquetConverter");
        job.getConfiguration().set("mapreduce.output.basename", confParser.conf.outputFilename);
        job.setJarByClass(ParquetConverter.class);

        job.setMapperClass(confParser.conf.inputFileFormat.equals("tsv") ? TSV : (confParser.conf.inputFileFormat.equals("csv") ? CSV : JSON));
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputKeyClass(Group.class);

        job.setOutputFormatClass(ExampleOutputFormat.class);
        ExampleOutputFormat.setSchema(job, MessageTypeParser.parseMessageType(schema));
        ExampleOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED);

        FileInputFormat.addInputPath(job, new Path(confParser.conf.inputDir));
        FileOutputFormat.setOutputPath(job, new Path(confParser.conf.outputDir));

        System.exit(job.waitForCompletion(true) ? 0: 1);

    }

}
