package shaochen.cube.util;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 提供单度量值元祖的数据存储和管理功能。
 * @author Shaochen
 *
 */
public class Member implements Comparable<Member>, Serializable {
	
	private static final long serialVersionUID = 1L;

	/**
	 * 存储各维度的值。
	 */
	private String[] dimensions;
	
	/**
	 * 获取维度长度。
	 * @return
	 */
	public int getDimensionCount() {
		return this.dimensions.length;
	}
	
	/**
	 * 当前的聚集标记。
	 */
	private int fromMark;
	
	/**
	 * 初始化shaochen.cube.util.Member类的新实例，但不生成成员值的文本描述。
	 * @param dimensions 各维度的值。
	 * @param fromMark 当前聚集标记。
	 */
	public Member(String[] dimensions, int fromMark) {
		this.dimensions = dimensions;
		this.fromMark = fromMark;
	}
	
	/**
	 * 重置当前成员为给定的聚集操作下的值。
	 * @param toMark 新的聚集标记。
	 * @return 重置后的Member实例。
	 */
	public Member reset(int toMark) {
		int r = this.fromMark ^ toMark;
		int pos = 0;
		while (r > 0) {
			if (r % 2 != 0) this.dimensions[pos] = "*";
			r = r / 2;
			pos ++;
		}
		return this;
	}
	
	/**
	 * 创建炸裂维的成员值。
	 * @param dimensions 待炸裂的成员值。
	 * @param divisionMark 炸裂维上的聚集标记。
	 * @return 炸裂的成员值。
	 */
	public String createDivisionValue(Integer divisionMark) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < dimensions.length; i++) {
			sb.append((divisionMark % 2) == 0 ? "*" : dimensions[i]).append("|");
			divisionMark = divisionMark / 2;
		}
		return sb.toString();
	}
	
	@Override
	public Member clone() {
		Member m = null;
		try {
			m = (Member) super.clone();
			if (m != null) m.dimensions = this.dimensions.clone(); //深度复制
		} catch (CloneNotSupportedException e) {
			m = new Member(this.dimensions.clone(), this.fromMark);
		}
		return m;
	}

	public int compareTo(Member o) {
		return this.toString().compareTo(o.toString());
	}
	
	@Override
	public boolean equals(Object other) {
		if (other instanceof Member) {
			Member r = (Member) other;
			return Arrays.equals(this.dimensions, r.dimensions);
		}
		return false;
	}
	
	/**
	 * 判断当前记录是否能由给定记录的泛化输出。
	 * @param other 潜在的父记录。
	 * @return true为存在泛化关系，反之为false。
	 */
	public boolean isChildOf(Member other) {
		for (int i = 0; i < this.dimensions.length; i++) {
			if (!this.dimensions[i].equals("*") && !this.dimensions[i].equals(other.dimensions[i])) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 缓存当前实例的hash值。
	 */
	private int hashCode = 0;
	
	@Override
	public int hashCode() {
		if (this.hashCode == 0) {
			int h = 0;
			for (String s : this.dimensions) {
				h += s.hashCode(); 
			}
			this.hashCode = h;
		}
		return this.hashCode;
	}
	
	/**
	 * 缓存文本描述。
	 */
	private String description = null;
	
	@Override
	public String toString() {
		if (description == null) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < this.dimensions.length; i++) {
				sb.append(this.dimensions[i] + "|");
			}
			this.description = sb.toString();
		}
		return this.description;
	}

}
