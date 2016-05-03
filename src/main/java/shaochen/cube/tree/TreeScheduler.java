package shaochen.cube.tree;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import shaochen.cube.plan.BinaryTree;
import shaochen.cube.plan.Cuboid;
import shaochen.cube.plan.LatticeIO;
import shaochen.cube.plan.PlanIO;

/**
 * 提供将搜索格所有格点生成树型执行计划的功能。
 * @author Shaochen
 *
 */
public class TreeScheduler {

	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		return new Options().addOption(input).addOption(output);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.buc.BucScheduler", options);
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
		//解析命令行参数
		Options options = TreeScheduler.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			TreeScheduler.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		String outputPath = cmd.getOptionValue("o");
		
		//加载搜索格数据
		Map<Integer, Long> cuboids = LatticeIO.loadFrom(inputPath);
		List<Map.Entry<Integer, Long>> unused = new ArrayList<Map.Entry<Integer, Long>>(cuboids.entrySet());
		unused.sort(new Comparator<Entry<Integer, Long>>() {

			public int compare(Entry<Integer, Long> o1, Entry<Integer, Long> o2) { //保证父格点始终在子格点前
				return o2.getKey().compareTo(o1.getKey());
			}
			
		});
		
		//绘制树型执行计划
		List<BinaryTree<Cuboid>> used = new ArrayList<BinaryTree<Cuboid>>(unused.size());
		BinaryTree<Cuboid> root = new BinaryTree<Cuboid>(new Cuboid(unused.remove(0)));
		used.add(root);
		while (unused.size() > 0) {
			BinaryTree<Cuboid> node = new BinaryTree<Cuboid>(new Cuboid(unused.remove(0)));
			for (BinaryTree<Cuboid> element : used) {
				if (node.getValue().isChildOf(element.getValue())) { //添加到父格点的子树下
					if (element.getLeft() == null) {
						element.setLeft(node);
					} else {
						BinaryTree<Cuboid> sibing = element.getLeft();
						while (sibing.getRight() != null) {
							sibing = sibing.getRight();
						}
						sibing.setRight(node);
					}
					used.add(node);
					break;
				}
			}
		}

		//保存Cube生成的执行计划
		try {
			PlanIO.printPipelines(root, System.out);
			PlanIO.unloadTo(root, outputPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
