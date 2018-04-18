import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeYear {

  public static class CrimeMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String crime = value.toString();
      String[] crimePri = crime.split(",");
      String[] dateTime = crimePri[2].split(" ");
      if (dateTime.length < 2) {
        return;
      }
      String[] date = dateTime[0].split("/");
      String year = date[2];
      String month = date[1];
      int int_year = Integer.parseInt(year);
      Configuration conf = context.getConfiguration();
      int check_year = Integer.parseInt(conf.get("year"));
      // System.out.println("[DEBUG]_____"+conf.get("year"));
      if (int_year == check_year) {
          Text out = new Text(crimePri[5]);
          context.write(out, one);
      }
    }
  }

  public static class CrimeReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("year", args[2]);
    Job job = Job.getInstance(conf, "word count");
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
