package shaochen.cube.batch;

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
 * 提供抽样全局数据，并将搜索格划分为批次的功能。
 * @author Shaochen
 *
 */
public class BatchScheduler {

	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option dims = new Option("d", true, "维度数量"); dims.setArgName("dimensionCount");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		return new Options().addOption(input).addOption(dims).addOption(output);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.batch.BatchScheduler", options);
	}
	
	public static void main(String[] args) {
		//解析命令行参数
		Options options = BatchScheduler.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			BatchScheduler.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//配置Spark上下文
		StringBuilder appName = new StringBuilder("BatchScheduler").append(" -i " + inputPath);
		SparkConf conf = new SparkConf().setAppName(appName.toString());
		JavaSparkContext context = new JavaSparkContext(conf);
		
		Map<Integer, Long> cuboids = LatticeSampler.estimateCuboidSize(context, inputPath, dimensionCount); //估算格点成本
		int batchSize = BatchScheduler.getBatchSize(context, inputPath); //估算各批次的格点数量
		BinaryTree<shaochen.cube.plan.Cuboid> pipelines = BalanceCluster.createPipeLines(cuboids, batchSize); //生成批次划分方案
		
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
	 * 获取可行的批次大小，使得每批次的数据和映射的数据可容于集群内存。
	 * @param context Spark程序上下文。
	 * @return 批次大小。
	 * @throws IOException 
	 */
	private static int getBatchSize(JavaSparkContext context, String hdfsPath) {
		double clusterMemory = BatchScheduler.getExecutionCapacity(context.getConf());
		try {
			double fileLength = BatchScheduler.getFileLength(hdfsPath) * MetaInfo.GROWTH_FACTOR;
			return (int) (clusterMemory / fileLength);
		} catch (IOException e) {
			e.printStackTrace();
			return 1;
		}
	}
	
	/**
	 * 获取可用于执行的集群内存大小。
	 * @param conf Spark配置文件。
	 * @return 文件大小，单位MB。
	 */
	private static double getExecutionCapacity(SparkConf conf) {
		double clusterMemory = BatchScheduler.getExecutorMemory(conf) * MetaInfo.CLUSTER_SIZE;
		return (1 - conf.getDouble("spark.memory.storageFraction", 0.5)) * clusterMemory;
	}

	/**
	 * 获取计算节点的内存大小。
	 * @param conf Spark配置文件。
	 * @return 文件大小，单位MB。
	 * @throws IOException
	 */
	private static double getExecutorMemory(SparkConf conf) {
		String value = conf.get("spark.executor.memory");
		double size = Double.parseDouble(value.substring(0, value.length() - 1));
		String unit = value.substring(value.length() - 1).toLowerCase();
		if (unit.equals("g")) {
			return size * MetaInfo.GB_TO_MB;
		} else { //默认单位为M
			return size;
		}
	}
	
	/**
	 * 获取HDFS文件的大小。
	 * @param hdfsPath 文件路径。
	 * @return 文件大小，单位MB。
	 * @throws IOException
	 */
	private static double getFileLength(String hdfsPath) throws IOException {
		Path p = new Path(hdfsPath);
		FileSystem hdfs = p.getFileSystem(new Configuration());
		return 1.0 * hdfs.getContentSummary(p).getLength() / MetaInfo.MB_TO_BYTE;
	}

}
