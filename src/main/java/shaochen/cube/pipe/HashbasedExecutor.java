package shaochen.cube.pipe;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import scala.Tuple2;
import shaochen.cube.pipe.plan.BinaryTree;
import shaochen.cube.pipe.plan.Cuboid;
import shaochen.cube.util.Member;

/**
 * 提供基于hash-based算法，在给定数据集上执行单条pipeline生成相应的Cube分块的功能。
 * @author Shaochen
 *
 */
public class HashbasedExecutor {
	
	/**
	 * 指向须执行的pipeline。
	 */
	private BinaryTree<Cuboid> root = null;
	
	/**
	 * 初始化shaochen.cube.pipe.hash.HashbasedExecutor类的新实例。
	 * @param root 单条pipeline。
	 */
	public HashbasedExecutor(BinaryTree<Cuboid> root) {
		this.root = root;
	}

	/**
	 * 根据所属的pipeline，为给定的数据集生成相应部分的Cube。
	 * @param records 数据集。
	 * @return Cube分块。
	 */
	public List<Tuple2<Member, Long>> generatePartialCube(Iterable<Entry<Member, Long>> records) {
		List<Tuple2<Member, Long>> list = new LinkedList<Tuple2<Member, Long>>();
		
		//单独计算给定pipeline的顶格点，以减少数据的复制和内存开销
		Map<Member, Long> map = new HashMap<Member, Long>();
		for (Entry<Member, Long> record: records) {
			Member key = record.getKey();
			Long value = map.getOrDefault(key, 0L) + record.getValue();
			map.put(key, value);
		}
		this.addMapToList(map, list);

		//深度优先遍历pipeline
		this.preOrderTraverse(this.root.getLeft(), map, list);
		return list;
	}
	
	private void preOrderTraverse(BinaryTree<Cuboid> node, Map<Member, Long> parent, List<Tuple2<Member, Long>> list) {
		if (node != null) {
			Map<Member, Long> map = new HashMap<Member, Long>();
			for (Entry<Member, Long> record: parent.entrySet()) {
				Member key = record.getKey().clone().reset(node.getValue().getMark());
				Long value = map.getOrDefault(key, 0L) + record.getValue();
				map.put(key, value);
			}
			this.addMapToList(map, list);
			
			parent.clear(); //清空父hash表
			this.preOrderTraverse(node.getLeft(), map, list);
		}
	}
	
	private void addMapToList(Map<Member, Long> src, List<Tuple2<Member, Long>> dst) {
		for (Entry<Member, Long> e : src.entrySet()) {
			dst.add(new Tuple2<Member, Long>(e.getKey(), e.getValue()));
		}
	}

}