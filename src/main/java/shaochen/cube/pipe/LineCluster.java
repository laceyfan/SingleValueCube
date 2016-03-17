package shaochen.cube.pipe;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import shaochen.cube.plan.BinaryTree;
import shaochen.cube.plan.Cuboid;

/**
 * 提供将搜索格划分为多个pipeline的功能，各批次内格点共享排序成本。
 * @author Shaochen
 *
 */
public class LineCluster {

	/**
	 * 对所有格点创建pipelines，并输出数据炸裂方案
	 * @param cuboids 待划分的格点集合。
	 * @param cardinalThreshold 数据炸裂的最低份数。
	 * @return 执行计划。
	 */
	public static BinaryTree<Cuboid> createPipeLines(Map<Integer, Long> cuboids, Long cardinalThreshold) {
		List<Entry<Integer, Long>> unused = new LinkedList<Entry<Integer, Long>>(cuboids.entrySet()); //未划分的格点集合

		//根据空间大小逆序所有的格点
		unused.sort(new Comparator<Entry<Integer, Long>>() {

			public int compare(Entry<Integer, Long> o1, Entry<Integer, Long> o2) { //保证父格点始终在子格点前
				int r = o2.getValue().compareTo(o1.getValue());
				if (r == 0) {
					r = o2.getKey().compareTo(o1.getKey());
				}
				return r;
			}
			
		});
		
		//剔除所有规模低于炸裂阈值的格点，组成一条单独的pipeline，采用暴力算法计算
		BinaryTree<Cuboid> root = null;
		Iterator<Entry<Integer, Long>> iter = unused.iterator();
		while (iter.hasNext()) {
			Cuboid cuboid = new Cuboid(iter.next());
			if (cuboid.getSize() < cardinalThreshold) {
				BinaryTree<Cuboid> node = new BinaryTree<Cuboid>(cuboid);
				if (root == null) {
					root = node;
					cuboid.addSize(0 - 2 * cuboid.getSize()); //暴力pipeline用头格点的size取负来标记
				} else {
					node.setLeft(root.getLeft());
					root.setLeft(node);
				}
				iter.remove();
			}
		}

		//将搜索格划分为多条pipeline
		BinaryTree<Cuboid> last = root;
		while (unused.size() > 0) {
			//选取成本最大的格点作为所属pipeline的头结点
			BinaryTree<Cuboid> node = new BinaryTree<Cuboid>(new Cuboid(unused.remove(0)));
			last.setRight(node);
			
			//挑出成本最大的子格点递归组成一条pipeline
			BinaryTree<Cuboid> parent = node;		
			Iterator<Entry<Integer, Long>> unusedIter = unused.iterator();
			while (unusedIter.hasNext()) {
				BinaryTree<Cuboid> current = new BinaryTree<Cuboid>(new Cuboid(unusedIter.next()));
				if (current.getValue().isChildOf(parent.getValue())) {
					parent.setLeft(current);
					parent = current;
					unusedIter.remove();
				}
			}
			
			last = node;
		}
		
		return root;
	}

}
