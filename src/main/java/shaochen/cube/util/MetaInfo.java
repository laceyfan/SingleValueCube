package shaochen.cube.util;

/**
 * 存储算法执行的元信息。
 * @author Shaochen
 *
 */
public class MetaInfo {
	
	/**
	 * 集群计算节点的数量。
	 */
	public static int CLUSTER_SIZE = 5;
	
	/**
	 * 磁盘上的多维表解析为内存中的{@linkplain shaochen.cube.util.Member Member}类后，数据在规模上的膨胀倍数。
	 */
	public static int GROWTH_FACTOR = 12;
	
	/**
	 * 炸裂后每份数据平均的大小，单位为MB。
	 */
	public static long DIVISION_SIZE = 64;
	
	/**
	 * 1MB = 1024*1024Byte。
	 */
	public static final long MB_TO_BYTE = 1 << 20;
	
	/**
	 * 1GB = 1024MB。
	 */
	public static final long GB_TO_MB = 1 << 10;

}
