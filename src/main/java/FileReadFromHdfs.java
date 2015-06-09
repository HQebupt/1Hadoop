/*
 * 通过JAVA直接读取HDFS中的时候，一定会用到FSDataInputStream类，
 * 通过FSDataInputStream以流的形式从HDFS读数据代码如下：
 * 功能：处理收集到的日志。
 * 从HDFS读出文件，逐行处理查找error的信息，重新写到本地文件中，“行号+含有error的行内容”。
 */
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileReadFromHdfs {
	public static boolean hasError(String str) {
		int flag1 = str.indexOf("Error");
		int flag2 = str.indexOf("ERROR");
		int flag3 = str.indexOf("error");
		if (flag1 == -1 && flag2 == -1 && flag3 == -1)
			return false;
		return true;
	}


	public static void main(String[] args) throws IOException {
		String dst = "hdfs://mommy:9000/huangq/HDFSlogs/dailyRolling/201305/mommy-dailyRolling-2013-05-06-.1367825162347";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));
		OutputStream out = new FileOutputStream(
				"D://Eclipse//WorkPlace//ProgramFiles//HDFSlog//log-error.txt");
		PrintStream printOut = new PrintStream(out);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				hdfsInStream));
		String strline ;
		StringBuffer strbuffer = new StringBuffer();
		int linenum = 0;
		while ((strline = br.readLine()) != null) {
			linenum++;
			if (FileReadFromHdfs.hasError(strline)) {
				strbuffer.append(linenum + ":" + strline + "\n");
				// System.out.println(strline); 
			}
		}
		printOut.write(strbuffer.toString().getBytes("utf-8"));
		System.out.println("ok");
		br.close();
		hdfsInStream.close();
		fs.close();
		printOut.close();
		out.close();
	}
}
