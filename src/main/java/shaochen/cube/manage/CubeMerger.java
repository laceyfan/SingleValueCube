package shaochen.cube.manage;

import java.io.IOException;
import java.lang.reflect.Member;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 提供合并两个Cube的功能。
 * @author Shaochen
 *
 */
public class CubeMerger {
	
	private static Options createCmdOptions() {
		Option input1 = new Option("i1", true, "方体1路径"); input1.setArgName("inputPath1");
		Option input2 = new Option("i2", true, "方体2路径"); input2.setArgName("inputPath2");
		Option output = new Option("o", true, "维度数量"); output.setArgName("dimensionCount");
		return new Options().addOption(input1).addOption(input2).addOption(output);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.manage.CubeMerger", options);
	}
	
	public static void main(String[] args) throws IOException {
		//解析命令行参数
		Options options = CubeMerger.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			CubeMerger.printHelp(options);
			return;
		}
		String inputPath1 = cmd.getOptionValue("i1") + (cmd.getOptionValue("i1").endsWith("/") ? "" : "/");
		String inputPath2 = cmd.getOptionValue("i2") + (cmd.getOptionValue("i2").endsWith("/") ? "" : "/");
		String outputDir = cmd.getOptionValue("o") + (cmd.getOptionValue("o").endsWith("/") ? "" : "/");

		//配置Spark上下文
		String appName = (new StringBuilder("CubeMerger")).append(" -i1 " + inputPath1).append(" -i2 " + inputPath2).toString();
		JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName(appName));
		
		//逐个合并子Cube
		Path p = new Path(inputPath1);
		FileSystem hdfs = p.getFileSystem(new Configuration());
		for (Path dir : FileUtil.stat2Paths(hdfs.listStatus(p))) {
			String mark = dir.getName();
			
			//加载数据
			JavaPairRDD<Member, Long> cube1 = context.sequenceFile(inputPath1 + mark, Member.class, Long.class);
			JavaPairRDD<Member, Long> cube2 = context.sequenceFile(inputPath2 + mark, Member.class, Long.class);
			
			//聚合方体
			cube1.union(cube2).reduceByKey(new Function2<Long, Long, Long>() {

				private static final long serialVersionUID = 1L;

				public Long call(Long v1, Long v2) throws Exception {
					return v1 + v2;
				}
				
			}).saveAsObjectFile(outputDir + mark);
		}
		
		context.close();
	}

}
