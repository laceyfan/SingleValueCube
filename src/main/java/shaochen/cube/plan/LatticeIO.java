package shaochen.cube.plan;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.Map;

public class LatticeIO {
	
	/**
	 * 加载搜索格。
	 * @param inPath 数据位置。
	 * @return 格点集合。
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws ClassNotFoundException 
	 */
	public static Map<Integer, Long> loadFrom(String inPath) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(inPath));
		@SuppressWarnings("unchecked")
		Map<Integer, Long> cuboids = (Map<Integer, Long>) in.readObject();
		in.close();
		return cuboids;
	}

	/**
	 * 存储搜索格。
	 * @param cuboids 格点集合。
	 * @param outPath 存储路径。
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void unloadTo(Map<Integer, Long> cuboids, String outPath) throws FileNotFoundException, IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(outPath));
		out.writeObject(cuboids);
		out.close();
	}
	/**
	 * 打印搜索格。
	 * @param cuboids 格点集合。
	 * @param out 输出流。
	 */
	public static void printLattcie(Map<Integer, Long> cuboids, PrintStream out) {
		for (Map.Entry<Integer, Long> entry : cuboids.entrySet()) {
			out.println(entry.getKey() + "=" + entry.getValue());
		}
	}

	/**
	 * 提供执行计划的读取并打印到标准输出的功能。
	 * @param args 命令行参数
	 * @author Shaochen
	 *
	 */
	public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException, IOException {
		if (args.length != 2) {
			System.out.println("用法：... -i <执行计划文件>");
			return;
		}
		LatticeIO.printLattcie(LatticeIO.loadFrom(args[1]), System.out);
	}

}
