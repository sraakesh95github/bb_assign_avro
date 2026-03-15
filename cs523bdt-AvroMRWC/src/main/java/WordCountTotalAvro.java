import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * WordCountTotalAvro is a Hadoop MapReduce application that performs word counting
 * using Avro for both intermediate map output and final reduce output.
 */
public class WordCountTotalAvro {
    /**
     * Mapper class that tokenizes input lines and emits (word, 1) pairs
     * using AvroKey and AvroValue wrappers.
     */
    public static class AvroWordCountMapper extends Mapper<LongWritable, Text, AvroKey<String>, AvroValue<Integer>> {
        private AvroKey<String> avroKey = new AvroKey<>();
        private AvroValue<Integer> avroValue = new AvroValue<>(1);

        /**
         * Maps a line of text into individual words.
         *
         * @param key     The byte offset of the line (ignored).
         * @param value   The text content of the line.
         * @param context The Hadoop job context.
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input text into tokens based on whitespace
            for (String token : value.toString().split("\\s+")) {
                if (!token.isEmpty()) {
                    avroKey.datum(token);
                    context.write(avroKey, avroValue);
                }
            }
        }
    }

    /**
     * Reducer class that aggregates counts for each word and emits
     * the final total count using Avro serialization.
     */
    public static class AvroWordCountReducer extends Reducer<AvroKey<String>, AvroValue<Integer>, AvroKey<String>, AvroValue<Integer>> {
        private AvroValue<Integer> avroValue = new AvroValue<>();

        /**
         * Sums up the counts for each word.
         *
         * @param key     The word wrapped in AvroKey.
         * @param values  The collection of counts for that word.
         * @param context The Hadoop job context.
         */
        @Override
        public void reduce(AvroKey<String> key, Iterable<AvroValue<Integer>> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Iterate over all values for the given word and sum them up
            for (AvroValue<Integer> value : values) {
                sum += value.datum();
            }

            avroValue.datum(sum);
            context.write(key, avroValue);
        }
    }

    /**
     * Main entry point for the Hadoop MapReduce application.
     * Configures and submits the MapReduce job.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WordCountTotalAvro <input path> <output path>");
            System.exit(-1);
        }
        // Check for a special "local" argument to run without a full HDFS/YARN cluster
        boolean isLocal = args.length > 2 && "local".equals(args[2]);

        Configuration conf = new Configuration();
        Job job;
        FileSystem.get(conf).delete(new Path(args[1]), true);

        // Configure the job for local execution if requested
        if (isLocal) {
            System.out.println("Running in local mode...");
            conf.set("fs.defaultFS", "file:///"); // Use local filesystem instead of HDFS
            conf.set("mapreduce.framework.name", "local"); // Use the local job runner
            job = Job.getInstance(conf, "local Avro word count");
        } else {
            job = Job.getInstance(conf, "Avro word count");
        }

        job.setJarByClass(WordCountTotalAvro.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(AvroWordCountMapper.class);
        job.setReducerClass(AvroWordCountReducer.class);

        //Need to set the output format class
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		

        // Need to set mapper output key and value schema
        AvroJob.setMapOutputKeySchema(job, Schema.create(Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Schema.create(Type.INT));
		
		
        // Need to set reducer output key and value schema
        AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Type.INT));

        // Set input and output paths from command line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
