package shaochen.cube.data;

import java.io.PrintWriter;

/**
 * 提供生成1000万条均匀数据集的功能。
 * @author Shaochen
 *
 */
public class UniformGenerator10M implements IDataGenerator {
	
	public void create3D(PrintWriter pw) {
		for (int a = 1; a <= 100; a++) {
			for (int b = 1; b <= 100; b++) {
				for (int c = 1; c <= 1000; c++) {
					pw.println(String.format("%d|%d|%d|1|", a, b, c));
				}
			}
		}
	}

	public void create4D(PrintWriter pw) {
		for (int a = 1; a <= 10; a++) {
			for (int b = 1; b <= 100; b++) {
				for (int c = 1; c <= 100; c++) {
					for (int d = 1; d <= 100; d++) {
						pw.println(String.format("%d|%d|%d|%d|1|", a, b, c, d));
					}
				}
			}
		}
	}

	public void create5D(PrintWriter pw) {
		for (int a = 1; a <= 10; a++) {
			for (int b = 1; b <= 10; b++) {
				for (int c = 1; c <= 10; c++) {
					for (int d = 1; d <= 100; d++) {
						for (int e = 1; e <= 100; e++) {
							pw.println(String.format("%d|%d|%d|%d|%d|1|", a, b, c, d, e));
						}
					}
				}
			}
		}
	}

	public void create6D(PrintWriter pw) {
		for (int a = 1; a <= 10; a++) {
			for (int b = 1; b <= 10; b++) {
				for (int c = 1; c <= 10; c++) {
					for (int d = 1; d <= 10; d++) {
						for (int e = 1; e <= 10; e++) {
							for (int f = 1; f <= 100; f++) {
								pw.println(String.format("%d|%d|%d|%d|%d|%d|1|", a, b, c, d, e, f));
							}
						}
					}
				}
			}
		}
	}

	public void create7D(PrintWriter pw) {
		for (int a = 1; a <= 10; a++) {
			for (int b = 1; b <= 10; b++) {
				for (int c = 1; c <= 10; c++) {
					for (int d = 1; d <= 10; d++) {
						for (int e = 1; e <= 10; e++) {
							for (int f = 1; f <= 10; f++) {
								for (int g = 1; g <= 10; g++) {
									pw.println(String.format("%d|%d|%d|%d|%d|%d|%d|1|", a, b, c, d, e, f, g));
								}
							}
						}
					}
				}
			}
		}
	}

}
