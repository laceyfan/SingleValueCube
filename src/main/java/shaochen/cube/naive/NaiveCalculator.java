package shaochen.cube.naive;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

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

import scala.Tuple2;
import shaochen.cube.util.MarkEnumeration;
import shaochen.cube.util.Member;

/**
 * 提供暴力生成Cube的功能。
 * @author Shaochen
 *
 */
public class NaiveCalculator {
	
	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		Option count = new Option("d", true, "维度数量"); count.setArgName("dimensionCount");
		return new Options().addOption(input).addOption(output).addOption(count);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.naive.NaiveCalculator", options);
	}
	
	public static void main(String[] args) {
		//解析命令行参数
		Options options = NaiveCalculator.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			NaiveCalculator.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//配置Spark上下文
		String appName = (new StringBuilder("NaiveCube")).append(" -i " + inputPath).toString();
		JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName(appName));
		
		//暴力计算每一个聚集操作
		int parallelism = context.getConf().getInt("spark.default.parallelism", 20);
		final Broadcast<Integer> bWidth = context.broadcast(dimensionCount);
		final Broadcast<Integer> bFromMark = context.broadcast((int) Math.pow(2, dimensionCount) - 1);
		JavaPairRDD<Member, Long> result = context.textFile(inputPath, parallelism).mapToPair(new PairFunction<String, Member, Long>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Member, Long> call(String t) throws Exception {
				String[] fields = t.split("\\|");
				int width = bWidth.value();
				Member member = new Member(Arrays.copyOf(fields, width), bFromMark.value());
				Long quantity = Long.parseLong(fields[width]);
				return new Tuple2<Member, Long>(member, quantity);
			}
			
		}).flatMapToPair(new PairFlatMapFunction<Tuple2<Member, Long>, Member, Long>() {

			private static final long serialVersionUID = 1L;

			public Iterable<Tuple2<Member, Long>> call(Tuple2<Member, Long> t) throws Exception {
				List<Tuple2<Member, Long>> list = new LinkedList<Tuple2<Member, Long>>();

				//为每一条记录生成对应于Cube中的2的n次方个元组
				Enumeration<Integer> enu = new MarkEnumeration(t._1().getDimensionCount());
				while (enu.hasMoreElements()) {
					list.add(new Tuple2<Member, Long>(t._1().clone().reset(enu.nextElement()), t._2()));
				}
				
				return list;
			}
			
		}).reduceByKey(new Function2<Long, Long, Long>() {

			private static final long serialVersionUID = 1L;

			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		//保存最终的Cube
		result.saveAsTextFile(cmd.getOptionValue("o"));
		context.close();
	}

}
