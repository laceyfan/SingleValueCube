package shaochen.cube.pipe;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import shaochen.cube.plan.BinaryTree;
import shaochen.cube.plan.Cuboid;
import shaochen.cube.plan.PlanIO;
import shaochen.cube.util.Member;

/**
 * 提供基于hash-based算法沿着线型pipeline生成Cube的功能。
 * @author Shaochen
 *
 */
public class PipeCalculator {
	
	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		Option count = new Option("d", true, "维度数量"); count.setArgName("dimensionCount");
		Option plan = new Option("p", true, "计划文件路径"); plan.setArgName("planPath");
		return new Options().addOption(input).addOption(output).addOption(count).addOption(plan);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.pipe.PipeCalculator", options);
	}
	
	public static void main(String[] args) {
		//解析命令行参数
		Options options = PipeCalculator.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			PipeCalculator.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		String outputDir = cmd.getOptionValue("o") + "/";
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//读取批次划分方案
		BinaryTree<Cuboid> plan = null;
		try {
			plan = PlanIO.loadFrom(cmd.getOptionValue("p"));
			if (plan == null) throw new NullPointerException("执行计划读取为空！");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		//配置Spark上下文
		String appName = (new StringBuilder("PipeCube")).append(" -i " + inputPath).toString();
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
					
		}).persist(StorageLevel.MEMORY_AND_DISK());

		//先用Naive算法计算低规模的pipeline
		BinaryTree<Cuboid> pipeline = plan;
		if (pipeline.getValue().getSize() < 0) { //暴力计算
			final Broadcast<BinaryTree<Cuboid>> bPipeline = context.broadcast(pipeline);
			table.flatMapToPair(new PairFlatMapFunction<Tuple2<Member, Long>, Member, Long>() {

				private static final long serialVersionUID = 1L;

				public Iterable<Tuple2<Member, Long>> call(Tuple2<Member, Long> t) throws Exception {
					List<Tuple2<Member, Long>> list = new LinkedList<Tuple2<Member, Long>>();

					//将当前元组映射为pipeline内的所有格点
					BinaryTree<Cuboid> node = bPipeline.value();
					while (node != null) {
						Member m = t._1().clone().reset(node.getValue().getMark());
						list.add(new Tuple2<Member, Long>(m, t._2()));							
						node = node.getLeft();
					}
					
					return list;
				}
			
			}).reduceByKey(new Function2<Long, Long, Long>() {

				private static final long serialVersionUID = 1L;

				public Long call(Long v1, Long v2) throws Exception {
					return v1 + v2;
				}
			
			}).saveAsObjectFile(outputDir + pipeline.getValue().getMark());
			
			//进入下一条pipeline
			bPipeline.destroy();
			pipeline = pipeline.getRight();
		}
		
		//逐个计算pipeline
		while (pipeline != null) {
			//找到炸裂的维度组合
			BinaryTree<Cuboid> node = pipeline;
			while (node.getLeft() != null) {
				node = node.getLeft();
			}
			
			//用hash-based计算
			final Broadcast<BinaryTree<Cuboid>> bPipeline = context.broadcast(pipeline);
			final Broadcast<Integer> bDivision = context.broadcast(node.getValue().getMark());
			table.mapToPair(new PairFunction<Tuple2<Member, Long>, String, Entry<Member, Long>>() {

				private static final long serialVersionUID = 1L;

				public Tuple2<String, Entry<Member, Long>> call(Tuple2<Member, Long> t) throws Exception {
					Cuboid s = bPipeline.value().getValue();
					Member m = t._1().clone().reset(s.getMark());
					String k = m.createDivisionValue(bDivision.value());
					return new Tuple2<String, Entry<Member, Long>>(k, new SimpleEntry<Member, Long>(m, t._2()));
				}
				
			}).groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Entry<Member, Long>>>, Member ,Long>() {

				private static final long serialVersionUID = 1L;

				public Iterable<Tuple2<Member, Long>> call(Tuple2<String, Iterable<Entry<Member, Long>>> t) throws Exception {
					return new HashbasedExecutor(bPipeline.value()).generatePartialCube(t._2());
				}
				
			}).saveAsObjectFile(outputDir + pipeline.getValue().getMark());
			
			//进入下一个pipeline
			bDivision.destroy();
			bPipeline.destroy();
			pipeline = pipeline.getRight();
		}
		
		context.close();
	}

}
