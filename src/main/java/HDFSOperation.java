///*
// * 功能：对于HDFS上的文件的常用操作。
// * 具体有：获取hosts名称、下载文件、删除文件、创建文件并写入内容、获取文件的内容及其属性信息（修改时间）。
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
//import java.util.*;
//
//public class HDFSOperation {
//	static String dst;
//	static Configuration conf;
//	static FileSystem fs;
//
//	private HDFSOperation() {
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
//	private static void getDirectoryFromHdfs()  {
//		try {
//			listDir(new Path(dst));
//
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
///**
// * 遍历文件的主要核心方法。
// * @param path
// * @throws IOException
// */
//	public static void listDir(Path path) throws IOException {
//		FileStatus[] inputFiles = fs.listStatus(path);
//		for(FileStatus file:inputFiles){
//            if(file.isDir()){
//            	System.out.println("DirName: "+file.getPath().getName());
//            	listDir(file.getPath());
//                continue;
//            }
//            System.out.println("fileName:"+file.getPath().getName()+"      Size："+file.getBlockSize());
//		}
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
//			// check if a file exists in HDFS
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
//			// create and write
//			System.out.println("create and write  " + filepath.getName()
//					+ "  to hdfs.");
//			FSDataOutputStream output = fs.create(filepath, true, 0);
//			output.writeUTF("testhdfs");
//			output.close();
//
//			// get the locations of a file in HDFS
//			System.out.println("Location of file in HDFS:");
//			FileStatus filestatus = fs.getFileStatus(filepath);
//			long filelength = filestatus.getLen();
//			BlockLocation[] blklocations = fs.getFileBlockLocations(filestatus,
//					0, filelength);
//			int blkCount = blklocations.length;
//			for (int i = 0; i < blkCount; i++) {
//				String[] hosts = blklocations[i].getHosts();
//				// do sth with the block hosts;
//				System.out.println(hosts);
//			}
//
//			// get HDFS file last modification time
//			long modtime = filestatus.getModificationTime();
//			Date date = new Date(modtime);
//			System.out.println(date);
//
//			// reading from HDFS
//			System.out
//					.println("read   " + filepath.getName() + "   from hdfs:");
//			FSDataInputStream fsinput = fs.open(filepath);
//			System.out.println(fsinput.readUTF());
//			fsinput.close();
//
//			System.out.println("遍历HDFS上的目录：");
//			HDFSOperation hdfsop = new HDFSOperation();
//			hdfsop.getDirectoryFromHdfs();
//			fs.close();
//		} catch (Exception e) {
//			// TODO: handle exception
//			e.printStackTrace();
//		}
//	}
//}
