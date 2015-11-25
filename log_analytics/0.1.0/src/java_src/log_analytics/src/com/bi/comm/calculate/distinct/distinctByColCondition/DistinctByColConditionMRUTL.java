/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercentilesByColConditionConditionMRUTL.java 
 * @Package com.bi.comm.calculate.percentiles.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-15 下午5:52:02 
 */
package com.bi.comm.calculate.distinct.distinctByColCondition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * @ClassName: PercentilesByColConditionConditionMRUTL
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-15 下午5:52:02
 */
public class DistinctByColConditionMRUTL {
	public static class DistinctByColConditionMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private String separator = "\t";

		// 需要distinct的列数
		private String[] distinctColNum = null;

		// 需要判断条件的列数
		private String[] conditionColNums = null;

		// 需要判断条件值 (不等于前@号)
		private String[] conditionValues = null;

		private enum CompareEnum {
			EQUAL, UNEQUAL, CONTAINS, LARGER, LESS
		};

		private int[] conditionCompareEnums = null;

		@Override
		public void setup(Context context) {
			// this.colNum =
			// context.getConfiguration().get("groupby").split(",");
			String separator = context.getConfiguration().get("separator");
			if (separator != null) {
				this.separator = separator;
			}
			System.out.println(this.separator + "|" + separator);
			this.distinctColNum = context.getConfiguration()
					.get("distinctcolindex").split(",");
			this.conditionColNums = context.getConfiguration()
					.get("conditioncol").split(",");
			this.conditionValues = context.getConfiguration()
					.get("conditionvalue").split(",");
			this.conditionCompareEnums = new int[this.conditionValues.length];
			for (int i = 0; i < this.conditionValues.length; i++) {
				if (this.conditionValues[i].startsWith("@")) {
					this.conditionCompareEnums[i] = CompareEnum.UNEQUAL
							.ordinal();
					this.conditionValues[i] = this.conditionValues[i]
							.substring(1);
				} else if (this.conditionValues[i].startsWith(">")) {
					this.conditionCompareEnums[i] = CompareEnum.LARGER
							.ordinal();
					this.conditionValues[i] = this.conditionValues[i]
							.substring(1);
				} else if (this.conditionValues[i].startsWith("<")) {
					this.conditionCompareEnums[i] = CompareEnum.LESS.ordinal();
					this.conditionValues[i] = this.conditionValues[i]
							.substring(1);
				} else if (this.conditionValues[i].contains("+")) {
					this.conditionCompareEnums[i] = CompareEnum.CONTAINS
							.ordinal();
				} else {
					this.conditionCompareEnums[i] = CompareEnum.EQUAL.ordinal();
				}
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringBuilder colKeySb = new StringBuilder();
			String[] field = value.toString().trim().split(this.separator);
			for (int i = 0; i < distinctColNum.length; i++) {
				if (field.length <= Integer.parseInt(distinctColNum[i])) {
					System.out.println("Input data error:" + value.toString());
					return;
				}
				colKeySb.append(field[Integer.parseInt(distinctColNum[i])]);
				if (i < distinctColNum.length - 1) {
					colKeySb.append("\t");
				}
			}

			boolean valid = true;

			for (int i = 0; i < this.conditionColNums.length; i++) {
				if (field.length <= Integer.parseInt(this.conditionColNums[i])) {
					return;
				}
				try {
					String colConditionString = field[Integer
							.parseInt(this.conditionColNums[i])];

					if (this.conditionCompareEnums[i] == CompareEnum.EQUAL
							.ordinal()) {
						if (!this.conditionValues[i]
								.equalsIgnoreCase(colConditionString)) {
							valid = false;
							break;
						}
					} else if (this.conditionCompareEnums[i] == CompareEnum.UNEQUAL
							.ordinal()) {
						if (this.conditionValues[i]
								.equalsIgnoreCase(colConditionString)) {
							valid = false;
							break;
						}
					} else if (this.conditionCompareEnums[i] == CompareEnum.LARGER
							.ordinal()) {
						long colConditionInteger = Long
								.parseLong(colConditionString);
						if (colConditionInteger <= Long
								.parseLong(this.conditionValues[i])) {
							valid = false;
							break;
						}
					} else if (this.conditionCompareEnums[i] == CompareEnum.LESS
							.ordinal()) {
						long colConditionInteger = Long
								.parseLong(colConditionString);
						if (colConditionInteger >= Long
								.parseLong(this.conditionValues[i])) {
							valid = false;
							break;
						}
					} else if (this.conditionCompareEnums[i] == CompareEnum.CONTAINS
							.ordinal()) {

						if (colConditionString.equalsIgnoreCase("")
								|| !this.conditionValues[i]
										.contains(colConditionString)) {
							valid = false;
							break;
						}
					}

				} catch (NumberFormatException e) {
					// TODO: handle exception
					System.out.println(e);
					return;
				}

			}
			if (valid) {
				context.write(new Text(colKeySb.toString()),
						new LongWritable(1));
			}
		}
	}

	public static class DistinctByColConditionReducer extends
			Reducer<Text, LongWritable, Text, Text> {
		private static Logger logger = Logger
				.getLogger(DistinctByColConditionReducer.class.getName());

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			int count = 0;
			for (LongWritable value : values) {
				count++;
			}
			context.write(key, new Text(Integer.toString(count)));

		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = new Job();
		job.setJarByClass(DistinctByColConditionMRUTL.class);
		job.setJobName("distinctByColMRUTL");
		job.getConfiguration().set("mapred.job.tracker", "local");
		job.getConfiguration().set("fs.default.name", "local");
		job.getConfiguration().set("distinctcolindex", "5");
		job.getConfiguration().set("conditioncol", "8");
		job.getConfiguration().set("conditionvalue", "0");
		FileInputFormat.setInputPaths(job, "input/inline_page");
		FileOutputFormat.setOutputPath(job, new Path("output/inline_page"));
		job.setMapperClass(DistinctByColConditionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(DistinctByColConditionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
