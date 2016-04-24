package shaochen.cube.plan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import shaochen.cube.util.MarkEnumeration;
import shaochen.cube.util.Member;
import shaochen.cube.util.MetaInfo;

/**
 * 提供抽样给定数据集并估算出搜索格上各点的空间成本的功能。
 * @author Shaochen
 *
 */
public class LatticeSampler {
	
	/**
	 * 在抽样给定数据集上估算出搜索格上各聚集操作的结果大小。
	 * @param context Spark程序上下文。
	 * @param inputPath 数据集的存储路径。
	 * @param dimensionCount 维度数量。
	 * @return 不同聚集任务的结果大小。
	 */
	public static Map<Integer, Long> estimateCuboidSize(JavaSparkContext context, String inputPath, int dimensionCount) {
		Map<Integer, Long> map = new HashMap<Integer, Long>();
		
		//配置采样参数
		double fraction = LatticeSampler.calculateSampleFraction(context, inputPath);
		int parallelism = context.getConf().getInt("spark.default.parallelism", 20);
		JavaRDD<String> lines = context.textFile(inputPath, parallelism).sample(false, fraction, new Random().nextInt());
		
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
		
		return map;
	}
	
	/**
	 * 计算合理的采样比例，使得采样数据可以放入集群的缓存中。
	 * @param context Spark程序上下文。
	 * @param hdfsPath 数据文件路径。
	 * @return 采用比例。
	 */
	private static double calculateSampleFraction(JavaSparkContext context, String hdfsPath) {
		double memoryCapacity = LatticeSampler.getStorageCapacity(context.getConf());
		try {
			double fileLength = LatticeSampler.getFileLength(hdfsPath) * MetaInfo.GROWTH_FACTOR;
			return Math.min(memoryCapacity / fileLength, 1.0);
		} catch (IOException e) {
			e.printStackTrace();
			return 1.0;
		}
	}
	
	/**
	 * 获取可用于缓存的集群内存大小。
	 * @param conf Spark配置文件。
	 * @return 文件大小，单位MB。
	 */
	private static double getStorageCapacity(SparkConf conf) {
		double clusterMemory = LatticeSampler.getExecutorMemory(conf) * MetaInfo.CLUSTER_SIZE;
		return conf.getDouble("spark.memory.storageFraction", 0.5) * clusterMemory;
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
