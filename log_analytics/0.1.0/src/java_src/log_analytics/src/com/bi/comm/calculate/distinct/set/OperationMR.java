/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: UserLostListMR.java 
 * @Package com.bi.client.lostuserfail 
 * @Description: 对日志名进行处理
 * @author niewf
 * @date 2013-8-7 上午2:24:09 
 * @input:输入日志路径/2013-8-7
 * @output:输出日志路径/2013-8-7
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.comm.calculate.distinct.set;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * @ClassName: UserLostListMR
 * @Description: 这里用一句话描述这个类的作用
 * @author niewf
 * @date 2013-8-7 上午2:22:00
 */
public class OperationMR {
	public static String LEFT_SET = "left_set";

	public static String RIGHT_SET = "right_set";

	public static class OperationMap extends
			Mapper<LongWritable, Text, Text, Text> {
		private String filePathStr = null;

		private String leftPathIdentify;

		private String separator = ",";

		private int column = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
			filePathStr = fileInputSplit.getPath().toUri().getPath();
			System.out.println(filePathStr);
			this.leftPathIdentify = context.getConfiguration().get("left");
			this.separator = context.getConfiguration().get("separator");
			int columnInt = 0;
			try {
				columnInt = Integer.parseInt(context.getConfiguration().get("column"));
			} catch (Exception e) {
				// TODO: handle exception
				columnInt = 0;
				System.out.println(e);
			}
			if(columnInt >= 0){
				this.column = columnInt;
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				UnsupportedEncodingException {
				String line = value.toString().trim();
				String field[] = line.split(this.separator);
				if(field.length <= this.column){
					System.out.println("Input fields out of length, field length:" + field.length +", column input:" + this.column);
					return;
				}
				String mac = field[this.column].toUpperCase();
				// left打上左标签
				if (this.filePathStr.contains(this.leftPathIdentify)) {
					context.write(new Text(mac), new Text(
							OperationMR.LEFT_SET));
				}
				// right打上右标签
				else {
					context.write(new Text(mac), new Text(
							OperationMR.RIGHT_SET));
				}
		}
	}

	public static class OperationReduce extends
			Reducer<Text, Text, Text, Text> {
		
		private enum SETOPERATION{AND, OR, SUB};
		private SETOPERATION setOperation = SETOPERATION.AND;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
	
			String operationString = context.getConfiguration().get("operation");
			if(operationString.equalsIgnoreCase("AND")){
				this.setOperation = SETOPERATION.AND;
			}else if(operationString.equalsIgnoreCase("OR")){
				this.setOperation = SETOPERATION.OR;
			}else if(operationString.equalsIgnoreCase("SUB")){
				this.setOperation = SETOPERATION.SUB;
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String valueStr = null;
			boolean label = false;
			boolean exLabel = false;
			for (Text val : values) {
				valueStr = val.toString();
				if (valueStr.contains(OperationMR.LEFT_SET)) {
					label = true;
				} else {
					exLabel = true;
				}
			}
			
			if (null == valueStr) {
				return;
			}
			
			if(this.setOperation.ordinal() == SETOPERATION.AND.ordinal()){
				if(label && exLabel){
					context.write(key, new Text("AND"));
				}
			}else if(this.setOperation.ordinal() == SETOPERATION.OR.ordinal()){
				if(label || exLabel){
					context.write(key, new Text("OR"));
				}
			}else if(this.setOperation.ordinal() == SETOPERATION.SUB.ordinal()){
				if(label &&  !exLabel){
					context.write(key, new Text("SUB"));
				}
			} 
			
		}
	}
}
