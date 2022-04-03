package com.HQ;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.RandomUtils;

import com.chart.LineChart;
import com.google.common.collect.Lists;
import com.timemanager.*;

public class HQMain {
	
	static Logger logger = LoggerFactory.getLogger(HQMain.class);	
	
	public static final int SIZE = 25;	//CPU在30左右超载
	public static final int CREDIT_LEVEL = 60;	//总分：100
	public static final int MIN_CONSENSUS_NUM = 4;  //最小共识节点数
	public static final int MAX_CONSENSUS_NUM = 20;  //最大共识节点数
	public static final int REQUEST_NUM = 300; //大于500便开始陷入
	public static long num = REQUEST_NUM;

	private static long lastTPS;
	private static List<Long> TPSList = new ArrayList<>();
	
	private static List<HQ> nodes = Lists.newArrayList();
	
	public static List<HQ> consensusNodes = Lists.newArrayList(); 
	public static List<HQ> candidateNodes = Lists.newArrayList(); 
	
	private static Random r = new Random();	
	
	private static long[][] delayNet = new long[SIZE][SIZE];	
	
	private static List<Long> costTimes = new ArrayList<>(); 

	
	public static void main(String[] args) throws InterruptedException {
		
		//初始化网络延迟
		for(int i=0;i<SIZE;i++){
			for(int j=0;j<SIZE;j++){
				if(i != j){
					// 随机延时
					delayNet[i][j] = RandomUtils.nextLong(10, 60);
				}else{
					delayNet[i][j] = 10;
				}
			}
		}	  
		
		//创建网络节点 标号第i个 总数size 非拜占庭 默认hq节点
		for(int i=0;i<SIZE;i++){
			nodes.add(new HQ(i,SIZE,false,true));
		}
		
		//共识节点,候选节点分类 并传入HQ类静态变量  
		classifyNodes(CREDIT_LEVEL,MAX_CONSENSUS_NUM);	
		HQ.setHQSize(consensusNodes.size());
		
		//多线程启动网络共识节点
		if (consensusNodes.size() < MIN_CONSENSUS_NUM) {
			System.out.println("Unenough creditable nodes, there are less than "
					+ MIN_CONSENSUS_NUM + "creditable nodes!");
		}else {		
			for(int i=0;i<consensusNodes.size();i++) {
				consensusNodes.get(i).start();
			}	
		}
		
		//全网节点随机产生请求   从node队列中的第random个节点，产生req请求，数据为字符串"testi"
		for(int i=0;i<REQUEST_NUM;i++){
			int node = r.nextInt(SIZE);
			nodes.get(node).req("test"+i);
		}			

		
		//定时计算网络吞吐量
		Timer TPSTimer = new Timer("TPS");
		TPSTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				if(costTimes.size() >0 && costTimes.size() < REQUEST_NUM) {
					lastTPS = costTimes.size() - lastTPS;
					TPSList.add(lastTPS);
				}
			}
		}, 0, 1000);   
		
		
		Thread.sleep(REQUEST_NUM*100);

		
		//console按编号输出执行时间
		System.out.println("请求运行时长：");
		System.out.println(costTimes);
		//		for(int i=0;i<costTimes.size();i++) {			
		//			System.out.println(i + ":" + costTimes.get(i));
		//		}
		//平均执行时间
		long timesTotal = 0;
		for(int i=0;i<costTimes.size();i++) {	
			timesTotal += costTimes.get(i);
		}
		System.out.println("平均执行时间：" + timesTotal/costTimes.size());
		//节点信息
		System.out.println("共识节点数：" + SIZE);
		//执行时间图表
    	LineChart costTimesChart = new LineChart(costTimes,"Request","Delay/ms","Exectimes");
	    SwingUtilities.invokeLater(() -> {    
			costTimesChart.setAlwaysOnTop(false);  
			costTimesChart.pack();  
			costTimesChart.setSize(600, 400);  
			costTimesChart.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);  
			costTimesChart.setVisible(true);  
	    });  
	    //吞吐量图表
	  	LineChart TPSChart = new LineChart(TPSList,"time/s","TPS","TPS");
	    SwingUtilities.invokeLater(() -> {    
	    	TPSChart.setAlwaysOnTop(false);  
	    	TPSChart.pack();  
	    	TPSChart.setSize(600, 400);  
	    	TPSChart.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);  
	    	TPSChart.setVisible(true);  
	    });  

		//定时判断请求是否已全部完成，如是，则输出测试数据
//		Timer outputTimer = new Timer("Output");
//		outputTimer.schedule(new TimerTask() {
//			@Override
//			public void run() {
//				if(costTimes.size() == num) {
//				    num++;
//					//console按编号输出执行时间
//					System.out.println("测试");
//					
//					//console按编号输出执行时间
//					System.out.println("请求运行时长：");
//					for(int i=0;i<costTimes.size();i++) {			
//						System.out.println(i + ":" + costTimes.get(i));
//					}
//					//平均执行时间
//					long timesTotal = 0;
//					for(int i=0;i<costTimes.size();i++) {	
//						timesTotal += costTimes.get(i);
//					}
//					System.out.println("平均执行时间：" + timesTotal/costTimes.size());
//					//节点信息
//					System.out.println("共识节点数：" + SIZE);
//					//执行时间图表
//			    	LineChart costTimesChart = new LineChart(costTimes,"Request","Delay/ms","Exectimes");
//				    SwingUtilities.invokeLater(() -> {    
//						costTimesChart.setAlwaysOnTop(false);  
//						costTimesChart.pack();  
//						costTimesChart.setSize(600, 400);  
//						costTimesChart.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);  
//						costTimesChart.setVisible(true);  
//				    });  
//				    //吞吐量图表
//				  	LineChart TPSChart = new LineChart(TPSList,"time/s","TPS","TPS");
//				    SwingUtilities.invokeLater(() -> {    
//				    	TPSChart.setAlwaysOnTop(false);  
//				    	TPSChart.pack();  
//				    	TPSChart.setSize(600, 400);  
//				    	TPSChart.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);  
//				    	TPSChart.setVisible(true);  
//				    });  
//
//				}
//			}
//		}, REQUEST_NUM*150, 2000); 
	    
	}
	
	
	//取满足可信度水平且不超过最大共识节点数的所有节点
	public static void classifyNodes(int creditLevel, int maxQuantity) {
//		Collections.reverse(nodes);   //集合降序排列----暂时不用
		int num = 0;
		for(int i=0;i<SIZE;i++) {
			if(nodes.get(i).getCredit() > creditLevel && num < maxQuantity) {
				consensusNodes.add(nodes.get(i));		//合规的节点加入共识类节点集合
				num++;
			}else {
				candidateNodes.add(nodes.get(i));
			}
		}
	}			
	
	public static void Bstart() {
		for(int i=0;i<candidateNodes.size();i++) {
			//candidateNodes.get(i).setHQ(true);
			candidateNodes.get(i).start();
		}
	}

	/**
	 * 向HQ节点广播消息
	 * @param msg
	 */
	public static void HQpublish(HQMsg msg){
		//logger.info("HQpublish广播消息[" +msg.getNode()+"]:"+ msg);
		for(HQ hq:consensusNodes){//在共识节点中的每一个节点，都把msg放入共识的qbm中
			// 模拟网络时延
			TimerManager.schedule(()->{
				hq.push(new HQMsg(msg));
				return null;
			}, delayNet[msg.getNode()][hq.getIndex()]);  //延迟选择中第一个函数为（消息本身处理的节点号，hq中节点标号）
		}
	}

	
	/**
	 * 向所有节点广播消息
	 * @param msg
	 */
	public static void publish(HQMsg msg){
//		logger.info("publish广播消息[" +msg.getNode()+"]:"+ msg);
		for(HQ hq:nodes){
			// 模拟网络时延
			TimerManager.schedule(()->{
				hq.push(new HQMsg(msg));
				return null;
			}, delayNet[msg.getNode()][hq.getIndex()]);
		}
	}
	
	/**
	 * 发送消息到指定节点
	 * @param toIndex
	 * @param msg
	 */	
	public static void send(int toIndex,HQMsg msg){
		// 模拟网络时延		
		TimerManager.schedule(()->{
			nodes.get(toIndex).push(msg);
			return null;
		}, delayNet[msg.getNode()][toIndex]);
	}	

	//收集Request处理时长
	public static void collectTimes(long costTime) {
		costTimes.add(costTime);
	}	

}
