// Sab import kar dunga, jhanjhat nahi chaiye mujhe.
import java.io.*;
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

class CrimeYear {

    public static class CrimeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text Crime;

        // Map each occurance of a crime with value 1
        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            String[] csv = line.split(",");
            String[] dateTime = csv[2].split(" ");
            String[] date = dateTime[0].split("/");
            String year = date[2];
            int int_year = Integer.parseInt(year);
            int check_year = Integer.parseInt(ctx.getConfiguration().get("year"));
            if (int_year == check_year) {
                ctx.write(Crime, one);
            }
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
        // Take year as input. Third arguement
        System.out.println(args);
        // conf.set("year", args[2]);

        Job job = Job.getInstance(conf, "Crime Projector");
        job.setJarByClass(CrimeYear.class);
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