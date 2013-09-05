/*
 * 功能：日志管理功能。
 * 实现的管理策略：1定时删除半年前的日志；2定时删除某类日志；3手动删除。
 * 删除文件和目录：
使用的是FileSystem对象的delete(Path f,boolean recursive)方法，布尔值设置为true时，才会删除一个目录。
 */

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class LogManager {
	Path dst;

	private LogManager() {
		dst = new Path("hdfs://mommy:9000/huangq/HDFSlogs");
	}

	public void setTest(String str) {
		dst = new Path(str);
	}

	public Path getdst() {
		return dst;
	}

	/** 遍历HDFS上的文件和目录 */
	private void getDirectoryFromHdfs(FileSystem fs, Path dst) {
		try {
			listDir(fs, dst);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 遍历文件的主要核心方法。
	 * 
	 * @param path
	 * @throws IOException
	 */
	public void listDir(FileSystem fs, Path path) throws IOException {
		FileStatus[] inputFiles = fs.listStatus(path);
		for (FileStatus file : inputFiles) {
			if (file.isDir()) {
				System.out.println("\nDirName: " + file.getPath().getName());
				listDir(fs, file.getPath());
				continue;
			}
			System.out.println("FName:" + file.getPath().getName() + "\nSize："
					+ file.getBlockSize());
		}
	}

	/**
	 * log管理：删除半年以前的日志。 参入参数：Path path. 功能：删除path下面日期在半年前的目录。
	 * 传入的path下的目录结果是201205、201206这样的目录，再下面便是这个月的日志文件。
	 */
	public boolean deleteHalfyear(FileSystem fs, Path path) {
		FileStatus[] inputFiles;
		DateFormat format1 = new SimpleDateFormat("yyyyMM");
		Date date = new Date();
		String datestr = new String();
		datestr = format1.format(date);
		int curdate = Integer.parseInt(datestr);
		try {
			boolean isExists = fs.exists(path);
			if (!isExists)
				return false;
			inputFiles = fs.listStatus(path);
			for (FileStatus file : inputFiles) {
				int filedate = Integer.parseInt(file.getPath().getName());
				int curyear = curdate / 100;
				int curmonth = curdate % 100;
				int fileyear = filedate / 100;
				int filemonth = filedate % 100;
				int tmp = (curyear - fileyear) * 12 + curmonth - filemonth;
				if (tmp >= 6)
					fs.delete(file.getPath(), true);
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * log管理：删除半年以前的日志。 参入参数：Path path
	 * ,path路径是日志存放的根目录。其下一级目录是日志种类；二级目录是日志的201205类的目录；三级目录是文件。
	 * 功能：删除path下面日期在半年前的目录。
	 */
	public boolean deleteHalfyearMain(FileSystem fs, Path path) {
		FileStatus[] inputFiles;
		try {
			inputFiles = fs.listStatus(path);
			for (FileStatus file : inputFiles) {
				if (file.isDir()) {
					deleteHalfyear(fs, file.getPath());
				}
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * log管理策略2：定时删除。传入参数：Path path；path是需要删除的目录或者文件
	 * 备注：这个方法可以采用Ozzie调用的方法，实现定时删除。
	 */
	public boolean deleteFile(FileSystem fs, Path path) {
		boolean isExists;
		try {
			isExists = fs.exists(path);
			if (isExists) {
				fs.delete(path, true);
			} else
				return false;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		LogManager hdfsop = new LogManager();
		try {
			// Get a list of all the nodes host names in the HDFS cluster
			FileSystem fs = FileSystem.get(conf);
			DistributedFileSystem hdfs = (DistributedFileSystem) fs;
			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
			int length = dataNodeStats.length;
			String[] hostnames = new String[length];
			System.out.println("列出集群的所有节点：");
			for (int i = 0; i < length; i++) {
				hostnames[i] = dataNodeStats[i].getHostName();
				System.out.println(hostnames[i]);
			}
			// 遍历HDFS上的目录
			System.out.println("遍历HDFS上的目录：");
			hdfsop.getDirectoryFromHdfs(fs, hdfsop.getdst());
			// 删除半年前的日志:传入的参数是日志种类
			Path filepath = new Path(
					"hdfs://mommy:9000/huangq/HDFSlogs/userlog");
			boolean isdelete = hdfsop.deleteHalfyear(fs, filepath);
			System.out.println("the half year ago files in "
					+ filepath.getName() + " is deleted?  :  " + isdelete);
			// 删除半年前的日志：传入的参数是日志的根目录
			isdelete = hdfsop.deleteHalfyearMain(fs, hdfsop.getdst());
			System.out
					.println("the half year ago files in LOG ROOT DIR is deleted?  :  "
							+ isdelete);
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
