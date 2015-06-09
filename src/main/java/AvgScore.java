import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class AvgScore {
  public static class AvgMapper extends
      Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) {
      String line = value.toString();
      String[] istr = line.split("\\s");
      String name = istr[0], score = istr[istr.length - 1];
      System.out.println(name + ":" + score);
      try {
        context.write(new Text(name),
            new IntWritable(Integer.parseInt(score)));
      } catch (NumberFormatException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static class AvgReducer extends
      Reducer<Text, IntWritable, Text, DoubleWritable> {
    private double avgscore;
    private int sum = 0, count = 0;

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) {
      for (IntWritable value : values) {
        sum += value.get();
        count++;
      }
      avgscore = sum / count;
      System.out.println(key.toString() + "  sum:" + sum + "  count:"
          + count + "  avgscore:" + avgscore);
      try {
        context.write(key, new DoubleWritable(avgscore));
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "10.1.1.240:9001");
    String[] ioArgs = new String[]{"/user/hadoop/huangq/ave_in",
        "/user/hadoop/huangq/ave_outd"};
    String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
        .getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println(" 输入输出路径参数错误。");
      System.exit(2);
    }
    /*
		 * 功能：配置作业。
		 */
    Job job = new Job(conf, "AvgScore");// 实例化一道作业
    job.setJarByClass(AvgScore.class);
    job.setMapperClass(AvgMapper.class);// (Mapper类型)
    job.setReducerClass(AvgReducer.class);// (Reducer类型)
    job.setOutputKeyClass(Text.class);// (map输出Key的类型);
    job.setOutputValueClass(IntWritable.class);// map(输出Value的类型);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));// 输入hdfs路径
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 输出hdfs路径
    System.exit(job.waitForCompletion(true) ? 0 : 1);// 退出的标志。
  }

}
