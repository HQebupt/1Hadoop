///*
// * 功能：日志管理功能。
// * 实现的管理策略：1定时删除半年前的日志；2定时删除某类日志；3手动删除。
// * 删除文件和目录：
//使用的是FileSystem对象的delete(Path f,boolean recursive)方法，布尔值设置为true时，才会删除一个目录。
// */
//
//import org.apache.hadoop.conf.*;
//import org.apache.hadoop.fs.*;
//import org.apache.hadoop.hdfs.*;
//import org.apache.hadoop.hdfs.protocol.*;
//import org.apache.hadoop.fs.FileStatus;
//
//import java.io.IOException;
//import java.net.URI;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//public class Test {
//	static String dst;
//	static Configuration conf;
//	static FileSystem fs;
//
//	private Test() {
//		dst = "hdfs://mommy:9000/huangq/HDFSlogs";
//		conf = new Configuration();
//		try {
//			fs = FileSystem.get(URI.create(dst), conf);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//	/** 遍历HDFS上的文件和目录 */
//	private static void getDirectoryFromHdfs() {
//		try {
//			listDir(new Path(dst));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * 遍历文件的主要核心方法。
//	 *
//	 * @param path
//	 * @throws IOException
//	 */
//	public static void listDir(Path path) throws IOException {
//		FileStatus[] inputFiles = fs.listStatus(path);
//		for (FileStatus file : inputFiles) {
//			if (file.isDir()) {
//				System.out.println("DirName: " + file.getPath().getName());
//				listDir(file.getPath());
//				continue;
//			}
//			System.out.println("FileName:" + file.getPath().getName()
//					+ "      Size：" + file.getBlockSize());
//		}
//	}
//
//	/**
//	 * log管理：删除半年以前的日志。 参入参数：Path path. 功能：删除path下面日期在半年前的目录。
//	 * 传入的path下的目录结果是201205、201206这样的目录，再下面便是这个月的日志文件。
//	 */
//	public static boolean deleteHalfyear(Path path) {
//		FileStatus[] inputFiles;
//		DateFormat format1 = new SimpleDateFormat("yyyyMM");
//		Date date = new Date();
//		String datestr = new String();
//		datestr = format1.format(date);
//		int curdate = Integer.parseInt(datestr);
//		try {
//			inputFiles = fs.listStatus(path);
//			for (FileStatus file : inputFiles) {
//				int filedate = Integer.parseInt(file.getPath().getName());
//				int curyear = curdate / 100;
//				int curmonth = curdate % 100;
//				int fileyear = filedate / 100;
//				int filemonth = filedate % 100;
//				int tmp = (curyear - fileyear) * 12 + curmonth - filemonth;
//				if (tmp >= 6) {
//					fs.delete(file.getPath(), true);
//				}
//			}
//			return true;
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return false;
//	}
//
//	/**
//	 * @param args
//	 */
//	public static void main(String[] args) {
//
//		Configuration conf = new Configuration();
//		try {
//			// Get a list of all the nodes host names in the HDFS cluster
//			FileSystem fs = FileSystem.get(conf);
//			DistributedFileSystem hdfs = (DistributedFileSystem) fs;
//			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
//			int length = dataNodeStats.length;
//			String[] hostnames = new String[length];
//			System.out.println("list of all the nodes in HDFS cluster:");
//			for (int i = 0; i < length; i++) {
//				hostnames[i] = dataNodeStats[i].getHostName();
//				System.out.println(hostnames[i]);
//			}
//
//			// 删除文件之前，判断是否存在。
//			Path filepath = new Path("/user/hadoop/huangq/input/input1.txt");
//			boolean isExists = fs.exists(filepath);
//			System.out.println("The file exists:" + isExists);
//			// if the file exit,delete it
//			if (isExists) {
//				boolean isDeleted = hdfs.delete(filepath, false);
//				if (isDeleted)
//					System.out.println("now delete:" + filepath.getName());
//			}
//
//			// get HDFS file last modification timewi
//			System.out.println("遍历HDFS上的目录：");
//			Test hdfsop = new Test();
//			hdfsop.getDirectoryFromHdfs();
//			boolean isdelete = hdfsop.deleteHalfyear(new Path(
//					"hdfs://mommy:9000/huangq/HDFSlogs/userlog"));
//			System.out.println(isdelete);
//			fs.close();
//		} catch (Exception e) {
//			// TODO: handle exception
//			e.printStackTrace();
//		}
//	}
//}
