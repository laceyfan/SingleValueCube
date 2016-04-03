package shaochen.cube.merge;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import shaochen.cube.util.Member;

/**
 * 提供将方体的记录解析为Tuple2<Member, Long>的功能。
 * @author Shaochen
 *
 */
public class RecordParser implements PairFunction<String, Member, Long> {

	private static final long serialVersionUID = 1L;

	public Tuple2<Member, Long> call(String t) throws Exception {
		String[] fields = t.substring(1, t.length() - 1).split(",");
		String[] dimensions = fields[0].split("|");
		Member member = new Member(dimensions, dimensions.length);
		Long quantity = Long.parseLong(fields[1]);
		return new Tuple2<Member, Long>(member, quantity);
	}

}
