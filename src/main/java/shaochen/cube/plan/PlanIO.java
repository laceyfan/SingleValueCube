package shaochen.cube.plan;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;

/**
 * 提供pipelines在本地的加载和存储的功能。
 * @author Shaochen
 *
 */
public class PlanIO {
	
	/**
	 * 加载执行计划。
	 * @param inPath 数据位置。
	 * @return pipelines。
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws ClassNotFoundException 
	 */
	public static BinaryTree<Cuboid> loadFrom(String inPath) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(inPath));
		@SuppressWarnings("unchecked")
		BinaryTree<Cuboid> root = (BinaryTree<Cuboid>) in.readObject();
		in.close();
		return root;
	}

	/**
	 * 存储执行计划。
	 * @param root pipelines。
	 * @param outPath 存储路径。
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void unloadTo(BinaryTree<Cuboid> root, String outPath) throws FileNotFoundException, IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(outPath));
		out.writeObject(root);
		out.close();
	}
	
	/**
	 * 以先序和中序分别打印执行计划。
	 * @param node pipelines。
	 * @param out 输出流。
	 */
	public static void printPipelines(BinaryTree<Cuboid> node, final PrintStream out) {
		out.print("先序：");
		PlanIO.preOrderPrint(node);
		out.print("\n中序：");
		PlanIO.inOrderPrint(node);
		out.println();
	}
	
	private static <T> void preOrderPrint(BinaryTree<T> tree) {
		if (tree != null) {
			System.out.print(tree.getValue() + ",");
			PlanIO.preOrderPrint(tree.getLeft());
			PlanIO.preOrderPrint(tree.getRight());
		}
	}
	
	private static <T> void inOrderPrint(BinaryTree<T> tree) {
		if (tree != null) {
			PlanIO.inOrderPrint(tree.getLeft());
			System.out.print(tree.getValue() + ",");
			PlanIO.inOrderPrint(tree.getRight());
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
		PlanIO.printPipelines(PlanIO.loadFrom(args[1]), System.out);
	}

}
