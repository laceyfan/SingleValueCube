package shaochen.cube.batch;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import shaochen.cube.plan.BinaryTree;
import shaochen.cube.plan.Cuboid;

/**
 * 提供将搜索格划分为多个批次的功能，各批次的规模相对均衡。
 * @author Shaochen
 *
 */
public class BalanceCluster {

	/**
	 * 对所有格点创建pipelines，并输出数据炸裂方案
	 * @param cuboids 待划分的格点集合。
	 * @param batchSize 批次大小。
	 * @return 执行计划。
	 */
	public static BinaryTree<Cuboid> createPipeLines(Map<Integer, Long> cuboids, int batchSize) {
		int batchCount = cuboids.size() / batchSize + 1; //批次数量
		List<Entry<Integer, Long>> unused = new LinkedList<Entry<Integer, Long>>(cuboids.entrySet()); //未划分的格点集合
		Set<BinaryTree<Cuboid>> headSet = new TreeSet<BinaryTree<Cuboid>>(new Comparator<BinaryTree<Cuboid>>() {

			public int compare(BinaryTree<Cuboid> o1, BinaryTree<Cuboid> o2) {
				int r = o1.getValue().getSize().compareTo(o2.getValue().getSize());
				if (r == 0) {
					r = o1.getValue().getMark().compareTo(o2.getValue().getMark());
				}
				return r;
			}
			
		}); //已划分的格点集合
		
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

		//选取成本最大的若干个格点作为所属批次的头结点
		BinaryTree<Cuboid> root = new BinaryTree<Cuboid>(new Cuboid(unused.get(0)));
		headSet.add(root);
		BinaryTree<Cuboid> last = root;
		for (int i = 1; i < batchCount; i++) {
			BinaryTree<Cuboid> current = new BinaryTree<Cuboid>(new Cuboid(unused.get(i)));
			headSet.add(current);
			
			//变量更替
			last.setRight(current);			
			last = current;
		}

		//将剩余的节点合并进各条pipeline中
		for (int i = batchCount; i < unused.size(); i++) {
			BinaryTree<Cuboid> current = new BinaryTree<Cuboid>(new Cuboid(unused.get(i)));
			
			//选择空间成本最小的头结点
			Iterator<BinaryTree<Cuboid>> usedIter = headSet.iterator();
			BinaryTree<Cuboid> head = usedIter.next();
			
			//并入头格点所属的批次
			current.setLeft(head.getLeft());
			head.setLeft(current);
			
			//更新头格点信息，并保集合的持规模正序
			usedIter.remove();
			head.getValue().addSize(current.getValue().getSize());
			headSet.add(head);
		}

		return root;
	}

}
