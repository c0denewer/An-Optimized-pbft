package com.pbft;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import com.chart.LineChart;
import com.timemanager.*;
  

public class PbftMain {

	static Logger logger = LoggerFactory.getLogger(PbftMain.class);
	
	public static final int SIZE = 20;
	public static final double BYZ_RATIO =0;  
	public static final int LIMITE_SIZE = 25; //CPU在30左右超载
	public static final int REQUEST_NUM = 300; //大于500便开始丢消息
	public static long num = REQUEST_NUM;

	private static long lastTPS;
	private static List<Long> TPSList = new ArrayList<>();

	
	private static long[][] delayNet = new long[LIMITE_SIZE][LIMITE_SIZE];	
	
	private static Random r = new Random();	
	
	private static List<Long> costTimes = new ArrayList<>(); 
	
	private static List<Pbft> nodes = Lists.newArrayList();
	
	
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
		
		//多线程启动网络节点
		for(int i=0;i<SIZE;i++){
			nodes.add(new Pbft(i,SIZE,BYZ_RATIO).start());
		}
		
		//全网节点随机产生请求
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
		
		//定时判断请求是否已全部完成，如是，则输出测试数据
		Timer outputTimer = new Timer("Output");
		outputTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				if(costTimes.size() == num) {
				    num++;
					//console按编号输出执行时间
//					System.out.println("测试");
					
					//console按编号输出执行时间
					System.out.println("请求运行时长：");
					System.out.println(costTimes);
//					for(int i=0;i<costTimes.size();i++) {			
//						System.out.println(i + ":" + costTimes.get(i));
//					}
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

				}
			}
		}, REQUEST_NUM*15, 2000); 

		
	    
	
	}

	/**
	 * 广播消息
	 * @param msg
	 */
	public static void publish(PbftMsg msg){
		//logger.info("publish广播消息[" +msg.getNode()+"]:"+ msg);
		for(Pbft pbft:nodes){
			// 模拟网络时延
			TimerManager.schedule(()->{
				pbft.push(new PbftMsg(msg));
				return null;
			}, delayNet[msg.getNode()][pbft.getIndex()]);
		}
	}
	
	/**
	 * 发送消息到指定节点
	 * @param toIndex
	 * @param msg
	 */	
	public static void send(int toIndex,PbftMsg msg){
		// 模拟网络时延
		TimerManager.schedule(()->{
			nodes.get(toIndex).push(new PbftMsg(msg));
			return null;
		}, delayNet[msg.getNode()][toIndex]);
	}
	
	public static void collectTimes(long costTime) {
		costTimes.add(costTime);
	}	

	
}