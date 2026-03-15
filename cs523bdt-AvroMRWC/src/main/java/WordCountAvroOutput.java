import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * WordCountAvroOutput is a Hadoop MapReduce application that uses Avro
 * serialization specifically for the final output stage.
 * In this job, the Mapper emits traditional Hadoop Writable types,
 * while the Reducer emits Avro-serialized key-value pairs.
 */
public class WordCountAvroOutput {

    /**
     * Mapper class for the WordCountAvroOutput job.
     * <p>
     * The input is a key-value pair where the key is the byte offset (LongWritable)
     * and the value is the line of text (Text).
     * The output uses standard Hadoop Writable types: Text for the word and IntWritable for the count (1).
     */
    public static class AvroWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * Tokenizes each line of text into words and emits each word with a count of 1.
         *
         * @param key     The byte offset of the line in the file (ignored).
         * @param value   The content of the line (record).
         * @param context The Hadoop context for job information and output.
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            // Iterate through tokens and emit each word with a count of 1
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                
				// Write mapper output here
            }
        }
    }

    /**
     * Reducer class for the WordCountAvroOutput job.
     * <p>
     * Receives a word (Text) and a list of counts (IntWritable) from the Mapper.
     * Aggregates the counts and emits the final result as an AvroKey and AvroValue.
     */
    public static class AvroWordCountReducer extends Reducer<Text, IntWritable, AvroKey<String>, AvroValue<Integer>> {
        private AvroKey<String> avroKey = new AvroKey<>();
        private AvroValue<Integer> avroValue = new AvroValue<>();

        /**
         * Aggregates the list of counts for a specific word and outputs the total count.
         *
         * @param key     The word received from the shuffle/sort phase.
         * @param values  The collection of counts (1s) for this word.
         * @param context The Hadoop context for job information and output.
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Aggregate all individual word counts
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Prepare and emit the result in Avro format            
			// Populate the avroKey and avroValue here
			
            context.write(avroKey, avroValue);
        }
    }

    /**
     * Main entry point for the Hadoop MapReduce application.
     * Configures and submits the MapReduce job.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WordCountAvroOutput <input path> <output path>");
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

        job.setJarByClass(WordCountAvroOutput.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(AvroWordCountMapper.class);
        job.setReducerClass(AvroWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);	// This is default format; this line could've been very well omitted!
		
		//job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		
		AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
		// AvroJob.setOutputValueSchema(job, Schema.create(Type.INT));
		

        // Set input and output paths from command line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}