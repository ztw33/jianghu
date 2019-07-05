package cn.nju.st13.pagerank;

import org.apache.hadoop.io.*;

import java.io.*;

public class PageRankElement implements Writable {
	//如果isNode是True, 那么传递的是图结构，content是所有的邻居
	//如果isNode是False，那么传递的是pagerank值
	private boolean isNode;
	private String content;
	private double passValue;

	//无参构造器是必须的
	//否则运行过程中会抛出NoSuchMethod <init>()异常
	public PageRankElement() {}

	public PageRankElement(double value) {
		isNode = false;
		content="";
		passValue=value;
	}

	public PageRankElement(String content) {
		isNode = true;
		this.content = content;
		passValue = 0;
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(isNode);
		dataOutput.writeUTF(content);
		dataOutput.writeDouble(passValue);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		isNode = dataInput.readBoolean();
		content = dataInput.readUTF();
		passValue = dataInput.readDouble();
	}

	public boolean isNode() {
		return isNode;
	}

	public void setNode(boolean node) {
		isNode = node;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getContent() {
		return content;
	}

	public double getPassValue() {
		return passValue;
	}

	public void setPassValue(double passValue) {
		this.passValue = passValue;
	}


	/*测试用
	public static void main(String[] args) throws Exception{
		String str = "狄云 戚方 1230";
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bout);
		out.writeDouble(1.5);
		out.writeUTF(str);

		DataInputStream in = new DataInputStream(new ByteArrayInputStream(bout.toByteArray()));
		Double x = in.readDouble();
		String str1 = in.readUTF();
		System.out.println(x.toString()+' '+str1);
	}
	*/
}