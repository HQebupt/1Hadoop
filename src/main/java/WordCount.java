import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		public Text word = new Text();

		// Object key没有用到，用到输入的文本value，把Context类理解成hadoop专用来写键值对的。
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String valuestring = value.toString();
			// System.out.print(valuestring + "...");
			/*
			 * 通过标准的输出可以看出map是一行一行处理的. stdout log：张三 88...李四 99...王五 66...赵六
			 * 77...张三 78...李四 89...王五 96...赵六 67...张三 80...李四 82...王五 84...赵六
			 * 86...
			 */
			String[] itr = valuestring.split("\\s");// 另外用split( " ")分词效果是一样的。
			// String分词，以空白符\s分词，注意转义。0可以分割多个空格,不知道为啥没有效果?????,这个参数控制一点效果都么有,在<0的时候。
			for (int i = 0; i < itr.length; i++) {
				if (itr[i] == null)
					System.out.println(itr[i] + "is  null.");
				else if (itr[i].equals("\0"))
					System.out.println(itr[i] + "is  \"\0\".");
				else if (itr[i].equals('\0'))
					System.out.println(itr[i] + "is '\0'.");
				else if (itr[i].equals("\\s"))
					System.out.println(itr[i] + "is  blank.");
				else if (itr[i].equals("")) {
					System.out.println(itr[i] + "is \"\".");
				} else
					System.out.println(itr[i] + "is a constant string.");
				if (itr[i].equals(""))
					continue;
				word.set(itr[i]);// Text的set()方法存放string单词。
				System.err.print(itr[i] + ",");/*
												 * 写到stderr logs里面去了。 stderr
												 * logs:
												 * 张三,88,李四,99,王五,66,赵六,77,
												 * 张三,78,
												 * 李四,89,王五,96,赵六,67,张三,80,
												 * 李四,82,王五,84,赵六,86,
												 */
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				System.out.println(key.toString() + ":" + sum + ";");
				/*
				 * 这样可以看出，reduce进行前，对map的结果进行了一个排序了。 stdout logs 66:1; 67:1;
				 * 77:1; 78:1; 80:1; 82:1; 84:1; 86:1; 88:1; 89:1; 96:1; 99:1;
				 * 张三:1; 张三:2; 张三:3; 李四:1; 李四:2; 李四:3; 王五:1; 王五:2; 王五:3; 赵六:1;
				 * 赵六:2; 赵六:3;
				 */
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	/**
	 * @param args
	 *            功能：读取hadoop配置，并检查运行命令是否正确。
	 *            详细：GenericOptionsParser是hadoop框架中解析命令行参数的基本类。
	 *            它能够辨别一些标准的命令行参数，能够使应用程序轻易地指定namenode，jobtracker ，以及其他额外的配置资源。
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "10.1.1.240:9001");
		String[] ioArgs = new String[] { "/user/hadoop/huangq/ave_in",
				"/user/hadoop/huangq/wc1_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println(" 输入输出路径参数错误。");
			System.exit(2);
		}
		/*
		 * 功能：配置作业。
		 */
		Job job = new Job(conf, "WordCount");// 实例化一道作业
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);// (Mapper类型)
		// job.setCombinerClass(IntsumReducer.class);
		job.setReducerClass(IntSumReducer.class);// (Reducer类型)
		job.setOutputKeyClass(Text.class);// (map输出Key的类型);
		job.setOutputValueClass(IntWritable.class);// (map输出Value的类型);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));// 输入hdfs路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 输出hdfs路径
		System.exit(job.waitForCompletion(true) ? 0 : 1);// 退出的标志。
		/* job.isSuccessful(); 或者job.isComplete(); */
	}

}
