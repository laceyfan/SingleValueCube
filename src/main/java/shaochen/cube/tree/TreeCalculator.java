package shaochen.cube.tree;

import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import shaochen.cube.plan.BinaryTree;
import shaochen.cube.plan.Cuboid;
import shaochen.cube.plan.PlanIO;
import shaochen.cube.util.Member;

/**
 * 提供复用父格点计算结果生成Cube的功能。
 * @author Shaochen
 *
 */
public class TreeCalculator {
	
	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		Option count = new Option("d", true, "维度数量"); count.setArgName("dimensionCount");
		Option plan = new Option("p", true, "计划文件路径"); plan.setArgName("planPath");
		return new Options().addOption(input).addOption(output).addOption(count).addOption(plan);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.buc.BucCalculator", options);
	}
	
	public static void main(String[] args) {
		//解析命令行参数
		Options options = TreeCalculator.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			TreeCalculator.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		String outputDir = cmd.getOptionValue("o") + (cmd.getOptionValue("o").endsWith("/") ? "" : "/");
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//读取树型执行计划
		BinaryTree<Cuboid> plan = null;
		try {
			plan = PlanIO.loadFrom(cmd.getOptionValue("p"));
			if (plan == null) throw new NullPointerException("执行计划读取为空！");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		//配置Spark上下文
		String appName = (new StringBuilder("BucCube")).append(" -i " + inputPath).toString();
		JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName(appName));

		//加载并解析文本数据
		int parallelism = context.getConf().getInt("spark.default.parallelism", 20);
		final Broadcast<Integer> bWidth = context.broadcast(dimensionCount);
		final Broadcast<Integer> bFromMark = context.broadcast((int) Math.pow(2, dimensionCount) - 1);
		JavaPairRDD<Member, Long> table = context.textFile(cmd.getOptionValue("i"), parallelism).mapToPair(new PairFunction<String, Member, Long>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Member, Long> call(String t) throws Exception {
				String[] fields = t.split("\\|");
				int width = bWidth.value();
				Member member = new Member(Arrays.copyOf(fields, width), bFromMark.value());
				long quantity = Long.parseLong(fields[width]);
				return new Tuple2<Member, Long>(member, quantity);
			}

		});
		
		//深度优先执行树型计划
		TreeCalculator.preOrderTraverse(context, table, plan, outputDir);
		context.close();
	}
	
	private static void preOrderTraverse(JavaSparkContext context, JavaPairRDD<Member, Long> input, BinaryTree<Cuboid> node, String outputDir) {
		if (node == null) {
			return;
		}
		
		//计算当前格点
		final Broadcast<Integer> bToMark = context.broadcast(node.getValue().getMark());
		JavaPairRDD<Member, Long> output = input.mapToPair(new PairFunction<Tuple2<Member, Long>, Member, Long>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Member, Long> call(Tuple2<Member, Long> t) throws Exception {
				Member m = t._1().clone().reset(bToMark.value());
				return new Tuple2<Member, Long>(m, t._2());
			}
			
		}).reduceByKey(new Function2<Long, Long, Long>() {

			private static final long serialVersionUID = 1L;

			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
			
		}).persist(StorageLevel.MEMORY_AND_DISK());
		output.saveAsObjectFile(outputDir + node.getValue().getMark());

		//执行其余格点
		TreeCalculator.preOrderTraverse(context, output, node.getLeft(), outputDir); //先执行子节点
		output.unpersist();
		TreeCalculator.preOrderTraverse(context, input, node.getRight(), outputDir); //执行兄弟节点
	}

}
