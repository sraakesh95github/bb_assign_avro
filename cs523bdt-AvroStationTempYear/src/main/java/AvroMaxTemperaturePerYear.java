import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * AvroMaxTemperaturePerYear aggregates the maximum temperature reported for each year
 * together with the station id that recorded it and writes the results as Avro records.
 */
public class AvroMaxTemperaturePerYear extends Configured implements Tool {

    private static Schema OUTPUT_SCHEMA;

    /**
     * Emits (year, stationId \t temperature) for every valid temperature reading.
     */
    public static class MaxTempMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            utils.parse(value);
            if (utils.isValidTemperature()) {
                outKey.set(utils.getYearInt());
                outValue.set(utils.getStationId() + "\t" + utils.getAirTemperature());
                context.write(outKey, outValue);
            }
        }
    }

    /**
     * For each year, finds the maximum temperature and emits a single Avro record.
     */
    public static class MaxTempReducer extends Reducer<IntWritable, Text, AvroKey<GenericRecord>, NullWritable> {

        @Override
        protected void reduce(IntWritable yearKey, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            float maxTemp = Float.NEGATIVE_INFINITY;
            String stationId = null;

            for (Text value : values) {
                String[] parts = value.toString().split("\\t", 2);
                if (parts.length != 2) {
                    continue;
                }
                float temperature = Float.parseFloat(parts[1]);
                if (stationId == null || temperature > maxTemp) {
                    maxTemp = temperature;
                    stationId = parts[0];
                }
            }

            if (stationId != null) {
                GenericRecord record = new GenericData.Record(OUTPUT_SCHEMA);
                record.put("year", yearKey.get());
                record.put("maxTemperature", maxTemp);
                record.put("stationId", stationId);
                context.write(new AvroKey<>(record), NullWritable.get());
            }
        }
    }

    /**
     * Ensures that reducer keys (years) are processed from latest to oldest.
     */
    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>%n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Avro Max Temperature Per Year");
        job.setJarByClass(AvroMaxTemperaturePerYear.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        OUTPUT_SCHEMA = new Schema.Parser().parse(new File(args[2]));

        // Clean existing output if present
        FileSystem.get(conf).delete(outputPath, true);

        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setSortComparatorClass(DescendingIntComparator.class);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job, OUTPUT_SCHEMA);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new AvroMaxTemperaturePerYear(), args);
        System.exit(res);
    }
}
