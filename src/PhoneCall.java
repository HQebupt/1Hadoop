import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PhoneCall {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text,Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s");
			context.write(new Text(line[1]),new Text( line[0]));
		}
	}

	public static class CallReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String out = "";
			for (Text value : values) {
				out += value.toString() + ",";
			}
			context.write(key, new Text(out));
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "10.1.1.240:9001");
		String[] ioArgs = new String[] { "/user/hadoop/huangq/phonecall_in",
				"/user/hadoop/huangq/phone_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println(" 输入输出路径参数错误。");
			System.exit(2);
		}
		Job job = new Job(conf, "PhoneCall");// 实例化一道作业
		job.setJarByClass(PhoneCall.class);
		job.setMapperClass(TokenizerMapper.class);// (Mapper类型)
		job.setReducerClass(CallReducer.class);// (Reducer类型)
		job.setOutputKeyClass(Text.class);// (map输出Key的类型);
		job.setOutputValueClass(Text.class);// (map输出Value的类型);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));// 输入hdfs路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 输出hdfs路径
		System.exit(job.waitForCompletion(true) ? 0 : 1);// 退出的标志。
	}
}
