package shaochen.cube.pipe;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import shaochen.cube.plan.BinaryTree;
import shaochen.cube.plan.LatticeSampler;
import shaochen.cube.plan.PlanIO;
import shaochen.cube.util.MetaInfo;

/**
 * 提供抽样全局数据，并将搜索格划分为pipeline的功能，各批次内格点共享排序成本。
 * @author Shaochen
 *
 */
public class PipeScheduler {

	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option dims = new Option("d", true, "维度数量"); dims.setArgName("dimensionCount");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		return new Options().addOption(input).addOption(dims).addOption(output);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.pipe.PipeScheduler", options);
	}
	
	public static void main(String[] args) {
		//解析命令行参数
		Options options = PipeScheduler.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			PipeScheduler.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//配置Spark上下文
		StringBuilder appName = new StringBuilder("PipeScheduler").append(" -i " + inputPath);
		SparkConf conf = new SparkConf().setAppName(appName.toString());
		JavaSparkContext context = new JavaSparkContext(conf);

		Map<Integer, Long> cuboids = LatticeSampler.estimateCuboidSize(context, inputPath, dimensionCount); //估算格点成本
		long threshold = PipeScheduler.calculateDivisionThreshold(inputPath); //计算炸裂阈值
		BinaryTree<shaochen.cube.plan.Cuboid> pipelines = LineCluster.createPipeLines(cuboids, threshold); //生成pipelines划分方案

		//保存Cube生成的执行计划
		try {
			PlanIO.printPipelines(pipelines, System.out);
			PlanIO.unloadTo(pipelines, cmd.getOptionValue("o"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		context.close();
	}
	
	/**
	 * 计算合理的炸裂数量，使得平均每份数据的大小在HDFS文件块的大小内。
	 * @param hdfsPath 数据文件路径。
	 * @return 炸裂阈值。
	 */
	private static long calculateDivisionThreshold(String hdfsPath) {
		long length = 0L;
		try {
			Path p = new Path(hdfsPath);
			FileSystem hdfs = p.getFileSystem(new Configuration());
			length = hdfs.getContentSummary(p).getLength() / MetaInfo.MB_TO_BYTE;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Math.max(length / MetaInfo.DIVISION_SIZE + 1, 20L);
	}

}
