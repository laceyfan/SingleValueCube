package shaochen.cube.plan;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * 提供对Cube搜索格的格点描述，包括聚集标记和空间大小。
 * @author Shaochen
 *
 */
public class Cuboid implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Integer mark;

	/**
	 * 获取格点的聚集标记。
	 * @return
	 */
	public Integer getMark() {
		return mark;
	}
	
	private Long size;

	/**
	 * 获取格点的空间大小。
	 * @return
	 */
	public Long getSize() {
		return size;
	}
	
	public void addSize(long delta) {
		this.size += delta;
	}

	/**
	 * 初始化shaochen.cube.util。Cuboid类的新实例。
	 * @param entry 格点统计结果。
	 */
	public Cuboid(Entry<Integer, Long> entry) {
		this.mark = entry.getKey();
		this.size = entry.getValue();
	}
	
	/**
	 * 判断当前格点是否能由给定格点的聚集输出。
	 * @param o 潜在的父格点。
	 * @return 是否存在父子关系。
	 */
	public boolean isChildOf(Cuboid o) {
		if (this.mark < o.getMark()) {
			int r = this.mark ^ o.getMark();
			if ((r & (r - 1)) == 0) return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return String.format("mark=%d&size=%d", this.mark, this.size);
	}

}
