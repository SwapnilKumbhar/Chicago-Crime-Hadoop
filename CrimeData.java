// Sab import kar dunga, jhanjhat nahi chaiye mujhe.
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class CrimeData {

    public static class CrimeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text Crime;

        // Map each occurence of a crime with value 1
        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            String[] crimes = line.split(",");
            Crime = new Text(crimes[5]);
            ctx.write(Crime, one);
        }
    }

    public static class CrimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable total = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            total.set(sum);
            ctx.write(key, total);
        }
    }

    public static void main(String... args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "crime data");
        job.setJarByClass(CrimeDataYear.class);
        job.setMapperClass(CrimeMapper.class);
        job.setCombinerClass(CrimeReducer.class);
        job.setReducerClass(CrimeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}