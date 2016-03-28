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
 * 提供生成均匀数据集的功能。
 * @author Shaochen
 *
 */
public class UniformData {

	private static Options createCmdOptions() {
		Option num = new Option("s", true, "表的行数");
		num.setArgName("tableSize");
		num.setRequired(true);
		
		Option dim = new Option("d", true, "维度数量");
		dim.setArgName("dimensionCount");
		dim.setRequired(true);
		
		Option out = new Option("o", true, "保存路径");
		out.setArgName("outputPath");
		out.setRequired(true);
		
		return new Options().addOption(num).addOption(dim).addOption(out);
	}
	
	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("java -cp SingleValueCube.jar shaochen.cube.data.UniformData", options);
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException, FileNotFoundException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		//解析命令行参数
		Options options = UniformData.createCmdOptions();
		CommandLine cmd = null;
		try {
			cmd = new BasicParser().parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			UniformData.printHelp(options);
			return;
		}
		String className = "UniformGenerator" + cmd.getOptionValue("s").toUpperCase();
		int dimensionCount = Integer.parseInt(cmd.getOptionValue("d"));
		String outputPath = cmd.getOptionValue("o");

		//生成指定的数据
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "utf-8"));
		IDataGenerator generator = (IDataGenerator) Class.forName(className).newInstance();
		switch (dimensionCount) {
			case 3: generator.create3D(pw); break;
			case 4: generator.create4D(pw); break;
			case 5: generator.create5D(pw); break;
			case 6: generator.create6D(pw); break;
			case 7: generator.create7D(pw); break;
			default: UniformData.printHelp(options);
		}
		pw.close();
	}

}
