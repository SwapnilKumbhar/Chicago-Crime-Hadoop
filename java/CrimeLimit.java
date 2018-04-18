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

public class CrimeLimit {

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
      int int_month = Integer.parseInt(month);
      Configuration conf = context.getConfiguration();

      String l_limit = conf.get("L_LIMIT");
      String[] l_lim = l_limit.split("/");
      int l_mon = Integer.parseInt(l_lim[0]);
      int l_year =  Integer.parseInt(l_lim[1]);

      String u_limit = conf.get("U_LIMIT");
      String[] u_lim = u_limit.split("/");
      int u_mon = Integer.parseInt(u_lim[0]);
      int u_year = Integer.parseInt(u_lim[1]);


      if (int_year >= l_year && int_year <= u_year) {
          if (int_month >= l_mon && int_month <= u_mon) {
            Text out = new Text(crimePri[5]);
            context.write(out, one);
          }
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
    conf.set("L_LIMIT", args[2]);
    conf.set("U_LIMIT", args[3]);
    
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(CrimeLimit.class);
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
