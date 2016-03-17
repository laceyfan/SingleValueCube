package shaochen.cube.plan;

import java.io.Serializable;

/**
 * 二叉树。
 * @author Shaochen
 *
 * @param <E> 元素类型。
 */
public class BinaryTree<E> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private BinaryTree<E> left = null;

	/**
	 * 获取左孩子。
	 * @return
	 */
	public BinaryTree<E> getLeft() {
		return left;
	}
	
	public void setLeft(BinaryTree<E> left) {
		this.left = left;
	}
	
	private BinaryTree<E> right = null;

	/**
	 * 获取右孩子。
	 * @return
	 */
	public BinaryTree<E> getRight() {
		return right;
	}

	public void setRight(BinaryTree<E> right) {
		this.right = right;
	}
	
	private E value = null;
	
	/**
	 * 获取当前节点。
	 * @return
	 */
	public E getValue() {
		return value;
	}

	/**
	 * 初始化shaochen.cube.pipe.util.BinaryTree类的新实例。
	 * @param node 待存储的节点。
	 */
	public BinaryTree(E value) {
		this.value = value;
	}

}
