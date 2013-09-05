/**
 * 实现了对某一linux目录下的文件，进行查error信息，写出的文件名是原始文件名+后缀".err"
 */
import java.io.*;

public class FindLinuxError {
	public static boolean hasError(String str) {
		int flag1 = str.indexOf("Error");
		if (flag1 == -1 )
			return false;
		return true;
	}

	public static boolean useful(String filename) {
		int flag1 = filename.indexOf(".xml");
		int flag2 = filename.indexOf(".crc");
		if (flag1 == -1 && flag2 == -1)
			return true;
		return false;
	}

	public void listDir(File dir) {
		File[] lists = dir.listFiles();
		// 打印当前目录下包含的所有子目录和文件的名字
		String info = "目录:" + dir.getName() + ":\n";
		int len = lists.length;
		for (int i = 0; i < len; i++)
			info += lists[i].getName() + "\n";
		System.out.println(info);
		// 打印当前目录下包含的所有子目录和文件的详细信息
		for (int i = 0; i < len; i++) {
			File f = lists[i];
			if (f.isFile())
				System.out.println("文件:" + f.getName() + "\n");
			else
				// 如果为目录，就递归调用listDir()方法
				listDir(f);
		}
	}

	/**
	 * 查找filepath文件的信息error
	 * 
	 * @param fs
	 * @param filepath
	 * @param logpath
	 * @throws IOException
	 */
	public void findFileError(File file, String logpath) throws IOException {
		OutputStream out = new FileOutputStream(logpath);
		PrintStream printOut = new PrintStream(out);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String strline = new String();
		StringBuffer strbuffer = new StringBuffer();
		int linenum = 0;
		while ((strline = br.readLine()) != null) {
			linenum++;
			if (FindLinuxError.hasError(strline)) {
				strbuffer.append(linenum + ":" + strline + "\n");
			}
		}
		printOut.write(strbuffer.toString().getBytes("utf-8"));
		br.close();
		printOut.close();
		out.close();
	}

	/**
	 * 查找该目录下文件的error
	 * 
	 * @param fs
	 * @param dirpath
	 * @throws IOException
	 */
	public void findDirError(File dir) throws IOException {
		File[] lists = dir.listFiles();
		// 打印当前目录下包含的所有子目录和文件的名字
		int len = lists.length;

		for (int i = 0; i < len; i++) {
			File f = lists[i];
			if (f.isFile()) {
				String filename = f.getName();
				if (FindLinuxError.useful(filename)) {
					String outpath = "/home/hadoop/hdfs3/huangq/logError/"
							+ f.getName() + ".err";
					findFileError(f, outpath);
				}
			} else
				// 如果为目录，就递归调用listDir()方法
				findDirError(f);
		}
	}

	public static void main(String[] args) throws IOException {
		FindLinuxError fb = new FindLinuxError();
		File dir = new File(
				"/home/hadoop/hadoop/logs/history/done/version-1/");
		// fb.listDir(dir);
		System.out.println("Completed！");
		fb.findDirError(dir);
	}
}
