package shaochen.cube.data;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * 提供均匀数据的创建功能。
 * @author Shaochen
 *
 */
public class UniformGenerator {

	private static Options createCmdOptions() {
		Option dims = new Option("d", true, "维度数量"); dims.setArgName("dimensionCount");
		Option output = new Option("o", true, "结果保存路径"); output.setArgName("outputPath");
		return new Options().addOption(dims).addOption(output);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.data.UniformGenerator", options);
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException, FileNotFoundException {
		//解析命令行参数
		Options options = UniformGenerator.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			UniformGenerator.printHelp(options);
			return;
		}
		
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(cmd.getOptionValue("o")), "utf-8"));
		switch (dimensionCount) {
			case 3: UniformGenerator.create3D(pw); break;
			case 4: UniformGenerator.create4D(pw); break;
			case 5: UniformGenerator.create5D(pw); break;
			case 6: UniformGenerator.create6D(pw); break;
			default: UniformGenerator.printHelp(options);
		}
		pw.close();
	}
	
	private static void create3D(PrintWriter pw) {
		for (int a = 1; a <= 100; a++) {
			for (int b = 1; b <= 100; b++) {
				for (int c = 1; c <= 1000; c++) {
					pw.println(String.format("%d|%d|%d|1|", a, b, c));
				}
			}
		}
	}
	
	private static void create4D(PrintWriter pw) {
		for (int a = 1; a <= 10; a++) {
			for (int b = 1; b <= 10; b++) {
				for (int c = 1; c <= 100; c++) {
					for (int d = 1; d <= 1000; d++) {
						pw.println(String.format("%d|%d|%d|%d|1|", a, b, c, d));
					}
				}
			}
		}
	}
	
	private static void create5D(PrintWriter pw) {
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
	
	private static void create6D(PrintWriter pw) {
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

}
