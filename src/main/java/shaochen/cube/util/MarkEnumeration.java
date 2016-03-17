package shaochen.cube.util;

import java.util.Enumeration;

/**
 * 提供枚举给定维度成员的所有group-by标记值的功能。
 * @author Shaochen
 *
 */
public class MarkEnumeration implements Enumeration<Integer> {
	
	/**
	 * 标记各维度是否为不限值。
	 */
	private boolean[] marks;
	
	/**
	 * 集合是否枚举到了尾部。
	 */
	private boolean noMoreElements;
	
	/**
	 * 初始化shaochen.cube.util.MarkEnumeration类的新实例。
	 * @param length 维度数量，0-31。
	 */
	public MarkEnumeration(int length) {
		this.marks = new boolean[length];
		this.noMoreElements = (length < 1);
	}

	public boolean hasMoreElements() {
		return !this.noMoreElements;
	}

	public Integer nextElement() {
		//构建新元素
		int mark = 0;
		this.noMoreElements = true;
		for (int i = 0; i < this.marks.length; i++) {
			if (this.marks[i]) mark |= (1 << i);
			this.noMoreElements = this.noMoreElements && this.marks[i];
		}

		//更新状态位
		for (int i = this.marks.length - 1; i >=0; i--) {
			this.marks[i] = !this.marks[i];
			if (this.marks[i]) break;
		}
		
		return mark;
	}

}
