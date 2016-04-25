package shaochen.cube.plan;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import shaochen.cube.util.MarkEnumeration;
import shaochen.cube.util.Member;

/**
 * 提供统计给定数据集搜索格上各点的空间成本的功能。
 * @author Shaochen
 *
 */
public class LatticeEvaluator {

	private static Options createCmdOptions() {
		Option input = new Option("i", true, "数据文件路径"); input.setArgName("inputPath");
		Option dims = new Option("d", true, "维度数量"); dims.setArgName("dimensionCount");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		return new Options().addOption(input).addOption(dims).addOption(output);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.pipe.LatticeEvaluator", options);
	}
	
	public static void main(String[] args) throws IOException {
		//解析命令行参数
		Options options = LatticeEvaluator.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			LatticeEvaluator.printHelp(options);
			return;
		}
		String inputPath = cmd.getOptionValue("i");
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));

		//配置Spark上下文
		StringBuilder appName = new StringBuilder("PipeScheduler").append(" -i " + inputPath);
		SparkConf conf = new SparkConf().setAppName(appName.toString());
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//配置采样参数
		int parallelism = context.getConf().getInt("spark.default.parallelism", 20);
		JavaRDD<String> lines = context.textFile(inputPath, parallelism);
		
		//解析数据并缓存
		final Broadcast<Integer> bWidth = context.broadcast(dimensionCount);
		final Broadcast<Integer> bFromMark = context.broadcast((int) Math.pow(2, dimensionCount) - 1);
		JavaRDD<Member> table = lines.map(new Function<String, Member>() {

			private static final long serialVersionUID = 1L;

			public Member call(String t) throws Exception {
				String[] fields = t.split("\\|");
				return new Member(Arrays.copyOf(fields, bWidth.value()), bFromMark.value());
			}
			
		}).persist(StorageLevel.MEMORY_AND_DISK());
		
		//逐个计算cuboid的大小		
		Map<Integer, Long> map = new HashMap<Integer, Long>();
		Enumeration<Integer> enu = new MarkEnumeration(dimensionCount);
		while (enu.hasMoreElements()) {
			Integer toMark = enu.nextElement();
			
			//按照给定的标记，执行一次聚集
			final Broadcast<Integer> bToMark = context.broadcast(toMark);
			long size = table.map(new Function<Member, Member>() {

				private static final long serialVersionUID = 1L;

				public Member call(Member t) throws Exception {
					return t.clone().reset(bToMark.value());
				}
			
			}).distinct().count();
			bToMark.destroy();
			
			map.put(toMark, size);
		}
		context.close();
		
		//存储搜索格的统计结果
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(new FileOutputStream(cmd.getOptionValue("o")));
			out.writeObject(map);
		} finally {
			out.close();
		}
	}

}
