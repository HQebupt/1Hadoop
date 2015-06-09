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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FindError {
	public static boolean hasError(String str) {
		int flag1 = str.indexOf("Error");
		// int flag2 = str.indexOf("ERROR");
		// int flag3 = str.indexOf("error");
		// if (flag1 == -1 && flag2 == -1 && flag3 == -1)
		if (flag1 == -1)
			return false;
		return true;
	}

	/**
	 * 查找filepath文件的信息error
	 * 
	 * @param fs
	 * @param filepath
	 * @param logpath
	 * @throws IOException
	 */
	public void findFileError(FileSystem fs, String filepath, String logpath)
			throws IOException {
		FSDataInputStream hdfsInStream = fs.open(new Path(filepath));
		OutputStream out = new FileOutputStream(logpath);
		PrintStream printOut = new PrintStream(out);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				hdfsInStream));
		String strline = new String();
		StringBuffer strbuffer = new StringBuffer();
		int linenum = 0;
		while ((strline = br.readLine()) != null) {
			linenum++;
			if (FindError.hasError(strline)) {
				strbuffer.append(linenum + ":" + strline + "\n");
			}
		}
		printOut.write(strbuffer.toString().getBytes("utf-8"));
		br.close();
		hdfsInStream.close();
		printOut.close();
		out.close();
		System.out.println("ok");
	}

	/**
	 * 查找filepath文件的信息error
	 * 
	 * @param fs
	 * @param filepath
	 * @param logpath
	 * @throws IOException
	 */
	public void findFileError(FileSystem fs, Path filepath, String logpath)
			throws IOException {
		/*
		 * 保留的副本代码 FSDataInputStream hdfsInStream = fs.open(filepath);
		 * OutputStream out = new FileOutputStream(logpath); PrintStream
		 * printOut = new PrintStream(out); BufferedReader br = new
		 * BufferedReader(new InputStreamReader( hdfsInStream)); String strline
		 * = new String(); StringBuffer strbuffer = new StringBuffer(); int
		 * linenum = 0; while ((strline = br.readLine()) != null) { linenum++;
		 * if (FindError.hasError(strline)) { strbuffer.append(linenum + ":" +
		 * strline + "\n"); } }
		 * printOut.write(strbuffer.toString().getBytes("utf-8"));
		 * 
		 * br.close(); hdfsInStream.close(); printOut.close(); out.close();
		 * System.out.println("ok");
		 */
		// 判断如果该文件没有错误的话，不生成error文件。
		FSDataInputStream hdfsInStream = fs.open(filepath);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				hdfsInStream));
		String strline = null;
		StringBuffer strbuffer = new StringBuffer();
		int linenum = 0;
		while ((strline = br.readLine()) != null) {
			linenum++;
			if (FindError.hasError(strline)) {
				strbuffer.append(linenum + ":" + strline + "\n");
			}
		}
		//System.out.println("strbuffer length:"+strbuffer.length());
		if (strbuffer.length()==0)
			System.out.println("ok");
		else {
			OutputStream out = new FileOutputStream(logpath);
			PrintStream printOut = new PrintStream(out);
			printOut.write(strbuffer.toString().getBytes("utf-8"));
			// 判断如果该文件没有错误的话，不生成error文件。
			br.close();
			hdfsInStream.close();
			printOut.close();
			out.close();
		}
	}

	/**
	 * 查找该目录下文件的error
	 * 
	 * @param fs
	 * @param dirpath
	 * @throws IOException
	 */
	public void findDirError(FileSystem fs, Path dirpath) throws IOException {
		FileStatus[] inputFiles = fs.listStatus(dirpath);
		for (FileStatus file : inputFiles) {
			if (file.isDir()) {
				System.out.println("DirPath: " + file.getPath());
				System.out.println("DirName: " + file.getPath().getName());
				findDirError(fs, file.getPath());
				continue;
			}
			Path filepath = file.getPath();
			String logpath = "D://Eclipse//WorkPlace//ProgramFiles//HDFSlog//userlog//"
					+ filepath.getName() + ".error";
			findFileError(fs, filepath, logpath);
			System.out.println("fileName:" + file.getPath().getName());
		}
	}

	public static void main(String[] args) throws IOException {
		String filedst = "hdfs://mommy:9000/huangq/HDFSlogs/dailyRolling/201305/mommy-dailyRolling-2013-05-06-.1367825162347";
		String dir = "hdfs://mommy:9000/huangq/HDFSlogs";
		Path dirpath = new Path(dir);
		// String logpath =
		// "D://Eclipse//WorkPlace//ProgramFiles//HDFSlog//log-error1.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filedst), conf);
		FindError ferr = new FindError();
		// ferr.findFileError(fs, filedst, logpath);
		ferr.findDirError(fs, dirpath);
		fs.close();
	}
}
