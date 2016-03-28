package shaochen.cube.data;

import java.io.PrintWriter;

/**
 * 定义生成多维数据集的操作。
 * @author Shaochen
 *
 */
public interface IDataGenerator {
	
	/**
	 * 创建3维数据集。
	 * @param pw 打印流。
	 */
	public void create3D(PrintWriter pw);

	/**
	 * 创建4维数据集。
	 * @param pw 打印流。
	 */
	public void create4D(PrintWriter pw);

	/**
	 * 创建5维数据集。
	 * @param pw 打印流。
	 */
	public void create5D(PrintWriter pw);

	/**
	 * 创建6维数据集。
	 * @param pw 打印流。
	 */
	public void create6D(PrintWriter pw);

	/**
	 * 创建7维数据集。
	 * @param pw 打印流。
	 */
	public void create7D(PrintWriter pw);

}
