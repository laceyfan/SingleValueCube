package shaochen.cube.dbms;

import java.util.Arrays;
import java.util.Enumeration;

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
import shaochen.cube.util.MarkEnumeration;
import shaochen.cube.util.Member;

/**
 * 提供枚举生成Cube的功能。
 * @author Shaochen
 *
 */
public class DbmsCalculator {
	
	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		Option count = new Option("d", true, "维度数量"); count.setArgName("dimensionCount");
		return new Options().addOption(input).addOption(output).addOption(count);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.dbms.DbmsCalculator", options);
	}
	
	public static void main(String[] args) {
		//解析命令行参数
		Options options = DbmsCalculator.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			DbmsCalculator.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		String outputDir = cmd.getOptionValue("o") + (cmd.getOptionValue("o").endsWith("/") ? "" : "/");
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//配置Spark上下文
		String appName = (new StringBuilder("DbmsCube")).append(" -i " + inputPath).toString();
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

		//逐个计算格点
		Enumeration<Integer> enu = new MarkEnumeration(dimensionCount);
		while (enu.hasMoreElements()) {
			Integer toMark = enu.nextElement();
			
			//按照给定的标记，执行一次聚集
			final Broadcast<Integer> bToMark = context.broadcast(toMark);
			table.mapToPair(new PairFunction<Tuple2<Member, Long>, Member, Long>() {

				private static final long serialVersionUID = 1L;

				public Tuple2<Member, Long> call(Tuple2<Member, Long> t) throws Exception {
					Member member = t._1().clone().reset(bToMark.value());
					Long quantity = t._2();
					return new Tuple2<Member, Long>(member, quantity);
				}
				
			}).reduceByKey(new Function2<Long, Long, Long>() {

				private static final long serialVersionUID = 1L;

				public Long call(Long v1, Long v2) throws Exception {
					return v1 + v2;
				}
			
			}).saveAsObjectFile(outputDir + toMark); //保存最终的Cube
			bToMark.destroy();
		}
		
		context.close();
	}

}
