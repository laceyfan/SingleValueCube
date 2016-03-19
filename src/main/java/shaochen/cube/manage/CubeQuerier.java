package shaochen.cube.manage;

import java.lang.reflect.Member;

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
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import shaochen.cube.pipe.plan.BinaryTree;
import shaochen.cube.pipe.plan.Cuboid;
import shaochen.cube.pipe.plan.PlanIO;

/**
 * 提供对Cube的查询功能。
 * @author Shaochen
 *
 */
public class CubeQuerier {
	
	private static Options createCmdOptions() {
		Option input = new Option("i", true, "方体存储目录"); input.setArgName("inputDir");
		Option plan = new Option("p", true, "计划文件路径"); plan.setArgName("planPath");
		Option mark = new Option("m", true, "视图聚集标记"); plan.setArgName("viewMark");
		Option condition = new Option("c", true, "度量值过滤条件，如<3和=4等"); plan.setArgName("filterCondition");
		Option output = new Option("o", true, "结果输出路径"); input.setArgName("outputPath");
		return new Options().addOption(input).addOption(plan).addOption(mark).addOption(condition).addOption(output);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.manage.CubeQuerier", options);
	}
	
	public static void main(String[] args) {
		//解析命令行参数
		Options options = CubeQuerier.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			CubeQuerier.printHelp(options);
			return;
		}

		//读取批次划分方案
		BinaryTree<Cuboid> plan = null;
		try {
			plan = PlanIO.loadFrom(cmd.getOptionValue("p"));
			if (plan == null) throw new NullPointerException("执行计划读取为空！");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		String pipelineMark = CubeQuerier.getSubDirectory(plan, Integer.parseInt(cmd.getOptionValue("m")));
		String inputPath = cmd.getOptionValue("i") + "/" + pipelineMark;
		
		//配置Spark上下文
		String appName = (new StringBuilder("PipeCube")).append(" -i " + inputPath).toString();
		JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName(appName));
		JavaPairRDD<Member, Long> view = context.sequenceFile(inputPath, Member.class, Long.class);
		
		//解析条件，执行过滤
		String condition = cmd.getOptionValue("c");
		if (condition.startsWith("=") || condition.startsWith("==")) {
			final Broadcast<Long> bValue = context.broadcast(Long.parseLong(condition.substring(1)));
			view.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return t._2().equals(bValue.value());
				}
				
			});
		} else if (condition.equals("!=") || condition.equals("<>")) {
			final Broadcast<Long> bValue = context.broadcast(Long.parseLong(condition.substring(12)));
			view.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return t._2().compareTo(bValue.value()) != 0;
				}
				
			});
		} else if (condition.equals(">")) {
			final Broadcast<Long> bValue = context.broadcast(Long.parseLong(condition.substring(12)));
			view.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return t._2().compareTo(bValue.value()) > 0;
				}
				
			});
		} else if (condition.equals(">=")) {
			final Broadcast<Long> bValue = context.broadcast(Long.parseLong(condition.substring(12)));
			view.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return t._2().compareTo(bValue.value()) >= 0;
				}
				
			});
		} else if (condition.equals("<")) {
			final Broadcast<Long> bValue = context.broadcast(Long.parseLong(condition.substring(12)));
			view.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return t._2().compareTo(bValue.value()) < 0;
				}
				
			});
		} else if (condition.equals("<=")) {
			final Broadcast<Long> bValue = context.broadcast(Long.parseLong(condition.substring(12)));
			view.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return t._2().compareTo(bValue.value()) <= 0;
				}
				
			});
		} else { //无法识别的操作符，返回空集
			view.filter(new Function<Tuple2<Member, Long>, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean call(Tuple2<Member, Long> t) throws Exception {
					return false;
				}
				
			});
		}
		
		//存储查询的结果
		view.saveAsTextFile(cmd.getOptionValue("o"));
		context.close();
		
	}
	
	/**
	 * 获取聚集视图所属的二级子目录名。
	 * @param plan 执行计划。
	 * @param viewMark 聚集视图的标记值。
	 * @return 子目录名。
	 */
	private static String getSubDirectory(BinaryTree<Cuboid> plan, int viewMark) {
		BinaryTree<Cuboid> node = plan;
		while (node != null) {
			if (node.getValue().getMark().equals(viewMark) || CubeQuerier.preOrderTraverse(node.getLeft(), viewMark)) {
				break;
			}
			node = node.getRight();
		}
		return node == null ? "-1" : node.getValue().getMark().toString();
	}
	
	private static boolean preOrderTraverse(BinaryTree<Cuboid> node, int mark) {
		if (node == null) return false;
		if (node.getValue().getMark().equals(mark)) {
			return true;
		}else {
			return CubeQuerier.preOrderTraverse(node.getLeft(), mark) && CubeQuerier.preOrderTraverse(node.getRight(), mark);
		}
	}

}
