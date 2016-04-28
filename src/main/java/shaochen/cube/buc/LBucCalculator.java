package shaochen.cube.buc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import shaochen.cube.plan.BinaryTree;
import shaochen.cube.plan.Cuboid;
import shaochen.cube.plan.PlanIO;
import shaochen.cube.util.Member;

/**
 * 提供基于BUC算法生成Cube的功能。
 * @author Shaochen
 *
 */
public class LBucCalculator {
	
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
		Options options = LBucCalculator.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			LBucCalculator.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		String outputDir = cmd.getOptionValue("o") + (cmd.getOptionValue("o").endsWith("/") ? "" : "/");
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//读取BUC执行树
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

		}).cache();
		
		//广度优先执行BUC计划
		Queue<Task> queue = new LinkedList<Task>();
		queue.offer(new Task(table, plan));
		while (queue.size() > 0) {
			Task task = queue.poll();
			BinaryTree<Cuboid> node = task.getNode();
			JavaPairRDD<Member, Long> input = task.getData();

			//重置维度值
			final Broadcast<Integer> bToMark = context.broadcast(node.getValue().getMark());
			JavaPairRDD<Member, Long> temp = input.mapToPair(new PairFunction<Tuple2<Member, Long>, Member, Long>() {

				private static final long serialVersionUID = 1L;

				public Tuple2<Member, Long> call(Tuple2<Member, Long> t) throws Exception {
					Member m = t._1().clone().reset(bToMark.value());
					return new Tuple2<Member, Long>(m, t._2());
				}
				
			}).cache();
			bToMark.destroy();

			//获取无效分区
			final List<Member> filtered = new LinkedList<Member>();
			temp.reduceByKey(new Function2<Long, Long, Long>() {

				private static final long serialVersionUID = 1L;

				public Long call(Long v1, Long v2) throws Exception {
					return v1 + v2;
				}
				
			}).collect().forEach(new Consumer<Tuple2<Member, Long>>() {

				public void accept(Tuple2<Member, Long> t) {
					if (t._2() > 0L) {
						filtered.add(t._1());
					}
				}
				
			});
			
			//过滤无效分区
			final Broadcast<List<Member>> bfiltered = context.broadcast(filtered);
			JavaPairRDD<Member, Long> output = temp.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return bfiltered.value().equals(t._1());
				}
				
			}).cache();
			bfiltered.destroy();
			temp.unpersist();
			output.saveAsObjectFile(outputDir + node.getValue().getMark());
			
			//添加子格点
			BinaryTree<Cuboid> child = node.getLeft();
			while (child != null) {
				queue.add(new Task(output, child));
				child = child.getRight();
			}
			
			//取消缓存
			if (!queue.peek().getData().equals(input)) {
				input.unpersist();
			}
		}
		context.close();
	}
	
	private static class Task implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		private JavaPairRDD<Member, Long> data;

		public JavaPairRDD<Member, Long> getData() {
			return data;
		}

		public BinaryTree<Cuboid> getNode() {
			return node;
		}
		
		private BinaryTree<Cuboid> node;
		
		public Task(JavaPairRDD<Member, Long> data, BinaryTree<Cuboid> node) {
			this.data = data;
			this.node = node;
		}
		
	}

}
