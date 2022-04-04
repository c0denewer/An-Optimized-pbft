package com.HQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;	
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.visitor.functions.Char;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;


public class HQ implements Comparable<HQ>{
	
	Logger logger = LoggerFactory.getLogger(getClass());
	
	public static final int CV = -2; // 视图变更
	public static final int VIEW = -1; // 请求视图
	
	public static final int HREQ = 0; //请求数据
	public static final int HPP = 1;  // 预准备阶段
	public static final int HBA = 2;  // 回复阶段
	public static final int HCON = 3; // 确认阶段
	public static final int HCOM = 4; // 回复		
	
	public static final int REQ = 5; // 请求数据
	public static final int PP = 6; // 预准备阶段
	public static final int PA = 7; // 准备阶段
	public static final int CM = 8; // 提交阶段
	public static final int REPLY = 9; // 回复

	
	public int size;    // 总节点数
	public static int HQSize; //共识节点数
	public int maxf;    // 最大失效节点
	private int index;  // 节点标识
	 
	private int view; // 视图view
	private volatile boolean viewOk = false; // 视图状态	
	private volatile boolean isRun;	  //isRun为false表示节点宕机、
	private volatile boolean isSleep;  //休眠即为不执行共识过程
	
	private volatile boolean isByzt;
	private volatile boolean isHQ;
	private volatile int credit;
	
	// 消息队列
	private BlockingQueue<HQMsg> qbm = Queues.newLinkedBlockingQueue();
	// 预准备阶段投票信息
	private Set<String> votes_pre = Sets.newConcurrentHashSet();
	
	// 准备阶段投票信息
	private Set<String> votes_pare = Sets.newConcurrentHashSet();
	private AtomicLongMap<String> aggre_pare = AtomicLongMap.create();
	
	// 提交阶段投票信息
	private Set<String> votes_comm = Sets.newConcurrentHashSet();
	private AtomicLongMap<String> aggre_comm = AtomicLongMap.create();
	// 成功处理过的请求
	private Map<String,HQMsg> doneReq = Maps.newConcurrentMap();
	// 作为主节点受理过的请求
	private Map<String,HQMsg> applyReq = Maps.newConcurrentMap();
	
	// 视图初始化 投票情况
	private AtomicLongMap<Integer> vnumAggreCount = AtomicLongMap.create();
	private Set<String> votes_vnum = Sets.newConcurrentHashSet();
	
	// 请求响应回复情况
	private AtomicLong replyCount = new AtomicLong();
	
	// pbft超时
	private Map<String,Long> timeOuts = Maps.newHashMap();
	
	// 请求超时，view加1，重试
	private Map<String,Long> timeOutsReq = Maps.newHashMap();
	
	//主节点收集Confirm超时
	private Map<String,Long> timeOutsBack = Maps.newHashMap();
	private Map<String,List<Integer>> timeOutsBackList = new HashMap<>();

	
	// 请求队列
	private BlockingQueue<HQMsg> reqQueue = Queues.newLinkedBlockingDeque(100);
	// 当前请求
	private HQMsg curReq;
	
	private volatile AtomicInteger genNo = new AtomicInteger(0); // 序列号生成
	
	private Timer timer;
	
	public HQ(int node,int size,boolean isBytz,boolean isHQ) {
		this.index = node;
		this.size = size;
		this.maxf = (size-1)/3;
		this.isByzt = isBytz;
		this.isHQ = isHQ;
		this.credit = 100;
		timer = new Timer("timer"+node);
	}
	
	public HQ start(){
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						//从消息队列中取出一个消息
						HQMsg msg = qbm.take();						
						doAction(msg);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}				
			}
		}).start();
		
		isRun = true;
		timer.schedule(new TimerTask() {
			int co = 0;
			@Override
			public void run() {
				if(isRun) {
					if(co == 0){
						// 启动后先同步视图
						pubView();
					}
					co++;
					doReq();
					checkHTimer();
				}

			}
		}, 10, 500);
		
		return this;
	}	
			
	/**
	 * 产生请求
	 * @param data
	 * @throws InterruptedException 
	 */
	public void req(String data) throws InterruptedException{
		HQMsg req = new HQMsg(HQ.HREQ, this.index);		
		req.setData(data);
		req.setOnode(index);
		reqQueue.put(req);
	}	
	
	/**
	 * 取出并标记请求
	 * @return
	 */
	protected boolean doReq() {
		if(!viewOk || curReq != null)return false; // 上一个请求还没发完/视图初始化中
		curReq = reqQueue.poll();
		if(curReq == null)return false;
		curReq.setVnum(this.view);
		curReq.setTime(System.currentTimeMillis());
		doSendCurReq();
		return true;
	}
	
	//发出请求
	void doSendCurReq(){
		timeOutsReq.put(curReq.getData(), System.currentTimeMillis());
		HQMain.send(getPriNode(view), curReq);
	}	

	protected boolean doAction(HQMsg msg) {
		if(!isRun) return false;
		if(isSleep) {
			switch (msg.getType()) {
			case HCOM:   
				onHReply(msg);
				break;
			case REPLY:
				onReply(msg);
				break;	
			case VIEW:
				onGetView(msg);
				break;			
			case CV:
				onChangeView(msg);
				break;
			default:
				break;
			}
			return false;
		}
		if(msg != null){
			//logger.info("收到消息[" +index+"]:"+ msg);
			switch (msg.getType()) {
			//HQPbft cases
			case HREQ:
				onHReq(msg);
				break;
			case HPP:
				onHPre(msg);
				break;
			case HBA:
				onHBack(msg);
				break;
			case HCON:
				onHCommit(msg);
				break;
			case HCOM:     /////////isRun为false, onHReply 也会取消 ！！！！！！！！
				onHReply(msg);
				break;
				
			// Pbft cases	
			case REQ:
				onReq(msg);
				break;			
			case PP:
				onPrePrepare(msg);
				break;			
			case PA:
				onPrepare(msg);
				break;			
			case CM:
				onCommit(msg);
				break;				
			case REPLY:
				onReply(msg);
				break;				
			case VIEW:
				onGetView(msg);
				break;			
			case CV:
				onChangeView(msg);
				break;
			default:
				break;
			}
			return true;
		}
		return false;
	}
	
	//第一步 HREQ 主节点广播信息
	private void onHReq(HQMsg msg) {

		if(!msg.isOk()) return;
		HQMsg sed = new HQMsg(msg);
		sed.setNode(index);//设置节点序号为消息的处理节点号
		if(msg.getVnum() < view) return;
		if(msg.getVnum() == view){  //若消息的视图号为  当前的消息号
			if(applyReq.containsKey(msg.getDataKey())) return; // 已经受理过
			applyReq.put(msg.getDataKey(), msg);
			timeOutsBack.put(msg.getData(), System.currentTimeMillis());
			// 主节点收到C的请求后进行广播
			sed.setType(HPP);//消息的种类设置为hq的预准备
			// 主节点生成序列号
			int no = genNo.incrementAndGet();
			sed.setNo(no);//把消息序列号+1的值设置为消息的序列号  ，在hqms中的
			HQMain.HQpublish(sed);//通过调用 main中的func：延迟一定时间后，把msg加入qbm队列
		}else if(msg.getNode() != index){ // 如果 消息的处理的节点不是现在这个节点						为什么是主节点收到才行
			// 非主节点收到，说明可能主节点宕机
			if(doneReq.containsKey(msg.getDataKey())){				
				// 已经处理过，直接回复
				sed.setType(REPLY);
				HQMain.send(msg.getNode(), sed);		//发给msg造出来的那个节点   对应图最后一列
			}else{
				// 认为客户端进行了CV投票 变更视图投票
				votes_vnum.add(msg.getNode()+"@"+(msg.getVnum()+1));
				vnumAggreCount.incrementAndGet(msg.getVnum()+1);	//消息视图号+1的那个投票数加1
				// 未处理，说明可能主节点宕机，转发给主节点试试
				//logger.info("转发主节点[" +index+"]:"+ msg);		//发给主节点
				HQMain.send(getPriNode(view), sed);
				timeOutsReq.put(msg.getData(), System.currentTimeMillis());
			}		
		}	
	}
	
	//第二步 Back 回复阶段，各个节点向主节点发送回复信息
	private void onHPre(HQMsg msg) {
		if(!checkMsg(msg,true)) return;
		String key = msg.getDataKey();
		if(votes_pre.contains(key))	return;
		
		votes_pre.add(key);
		// 启动超时控制
		timeOuts.put(key, System.currentTimeMillis());
		// 移除请求超时，假如有请求的话
		timeOutsReq.remove(msg.getData());
		// 进入准备阶段
		HQMsg sed = new HQMsg(msg);	//设置成hback类型的消息  消息的节点设成现在这个节点  用主函数里的send函数发给主节点priNode
		sed.setType(HBA);
		sed.setNode(index);
		if(isByzt) sed.setOk(false);  //拜占庭作恶
		HQMain.send(getPriNode(view), sed);	
	}	
	
	//第三步骤，主节点回复Confirm信息  
	private void onHBack(HQMsg msg) {
		if(!checkMsg(msg,false)) {
//			logger.info("异常消息[" +index+"]:"+msg);
			return;
		}
		
		//收集确认请求的节点信息
		if(timeOutsBackList.containsKey(msg.getData())) {
			timeOutsBackList.get(msg.getData()).add(msg.getNode());
		}else {
			List<Integer> BackList = new ArrayList<>();
			BackList.add(msg.getNode());
			timeOutsBackList.put(msg.getData(),BackList);
		}
		
		if(votes_pare.contains(msg.getKey()) && !votes_pre.contains(msg.getDataKey()))return;//准备投票有记录  预准备没有他  有错

		votes_pare.add(msg.getKey());		
		// 票数 +1
		long agCou = aggre_pare.incrementAndGet(msg.getDataKey());		//同意准备的投票里把消息的 （string@序列号）的票数+1
			if(agCou == HQSize){//如果等于hq节点个数  										
				aggre_pare.remove(msg.getDataKey());
				timeOutsBack.remove(msg.getData());
				for(int i=0;i<HQMain.consensusNodes.size();i++) {
					HQMain.consensusNodes.get(i).increCredit();
				}
				// 进入提交阶段
				HQMsg sed = new HQMsg(msg);
				sed.setType(HCON);
				sed.setNode(index);//把节点的序号给赋给消息的节点号
				doneReq.put(sed.getDataKey(), sed);	//加入doneReq队列
				HQMain.HQpublish(sed);//主函数中调用hq的push方法 ，加入节点自己的qbm队列
			}
			//主节点未得到所有投票情况，用超时机制处理
		
	}
		
	//第四步
	private void onHCommit(HQMsg msg) {
		if(!checkMsg(msg,false)) return;
		// data模拟数据摘要

		if(msg.getNode() != index){
			this.genNo.set(msg.getNo());//序列号设置为（消息的序列号）
		}
	
		HQMsg sed = new HQMsg(msg);
		sed.setType(HCOM);
		sed.setNode(index);
		// 回复客户端
		HQMain.send(sed.getOnode(), sed);//发给发出这个消息的节点  即客户端
	}
	
	//第五步
	private void onHReply(HQMsg msg) {

		if(curReq == null || !curReq.getData().equals(msg.getData()))return;
		long count = replyCount.incrementAndGet();
		if(count >= maxf+1){
			replyCount.set(0);
			curReq = null; // 当前请求已经完成
			// 执行相关逻辑					
			HQMain.collectTimes(msg.computCostTime());
			System.out.println("请求执行成功[" +index+"]" + "    " + "请求标识: [" +  msg.getData() + "]");
			
		}
	}
	
	
// PBFT source functions
	private void onReq(HQMsg msg) {
		if(!msg.isOk()) return;
		HQMsg sed = new HQMsg(msg);
		sed.setNode(index);
		if(msg.getVnum() < view) return;
		if(msg.getVnum() == index){
			if(applyReq.containsKey(msg.getDataKey())) return; // 已经受理过
			applyReq.put(msg.getDataKey(), msg);
			// 主节点收到C的请求后进行广播
			sed.setType(PP);
			sed.setVnum(view);
			// 主节点生成序列号
			int no = genNo.incrementAndGet();
			sed.setNo(no);
			HQMain.publish(sed);
		}else if(msg.getNode() != index){ // 自身忽略
			// 非主节点收到，说明可能主节点宕机
			if(doneReq.containsKey(msg.getDataKey())){
				// 已经处理过，直接回复
				sed.setType(REPLY);
				HQMain.send(msg.getNode(), sed);
			}else{
				// 认为客户端进行了CV投票
				votes_vnum.add(msg.getNode()+"@"+(msg.getVnum()+1));
				vnumAggreCount.incrementAndGet(msg.getVnum()+1);
				// 未处理，说明可能主节点宕机，转发给主节点试试
				logger.info("转发主节点[" +index+"]:"+ msg);
				HQMain.send(getPriNode(view), sed);
				timeOutsReq.put(msg.getData(), System.currentTimeMillis());
			}			
		}
	}	

	private void onPrePrepare(HQMsg msg) {
		if(!checkMsg(msg,true)) return;
		
		String key = msg.getDataKey();
		if(votes_pre.contains(key)){
			// 说明已经发起过，不能重复发起
			return;
		}
		votes_pre.add(key);
		// 启动超时控制
		timeOuts.put(key, System.currentTimeMillis());
		// 移除请求超时，假如有请求的话
		timeOutsReq.remove(msg.getData());
		// 进入准备阶段
		HQMsg sed = new HQMsg(msg);
		sed.setType(PA);
		sed.setNode(index);
		HQMain.publish(sed);
	}
	
	private void onPrepare(HQMsg msg) {
		if(!checkMsg(msg,false)) {
			logger.info("异常消息[" +index+"]:"+msg);
			return;
		}		
		String key = msg.getKey();
		if(votes_pare.contains(key) && !votes_pre.contains(msg.getDataKey())) return;
		
		votes_pare.add(key);		
		// 票数 +1
		long agCou = aggre_pare.incrementAndGet(msg.getDataKey());
		if(agCou >= 2*maxf+1){
			aggre_pare.remove(msg.getDataKey());
			// 进入提交阶段
			HQMsg sed = new HQMsg(msg);
			sed.setType(CM);
			sed.setNode(index);
			doneReq.put(sed.getDataKey(), sed);
			HQMain.publish(sed);
		}
		// 后续的票数肯定凑不满，超时自动清除			
	}
	
	private void onCommit(HQMsg msg) {
		if(!checkMsg(msg,false)) return;
		// data模拟数据摘要
		String key = msg.getKey();
		if(votes_comm.contains(key) && !votes_pare.contains(key)) return;
		
		votes_comm.add(key);
		// 票数 +1
		long agCou = aggre_comm.incrementAndGet(msg.getDataKey());
		if(agCou >= 2*maxf+1){
	
			remove(msg.getDataKey());
			if(msg.getNode() != index){
				this.genNo.set(msg.getNo());
			}
			// 进入回复阶段
			if(msg.getOnode() == index){
				// 自身则直接回复
				onReply(msg);
			}else{
				HQMsg sed = new HQMsg(msg);
				sed.setType(REPLY);
				sed.setNode(index);
				// 回复客户端
				HQMain.send(sed.getOnode(), sed);
			}			
		}
	}		

	private void onReply(HQMsg msg) {
		if(curReq == null || !curReq.getData().equals(msg.getData()))return;
		long count = replyCount.incrementAndGet();
		if(count >= maxf+1){
			replyCount.set(0);
			curReq = null; // 当前请求已经完成
			// 执行相关逻辑	
			HQMain.collectTimes(msg.computCostTime());
			System.out.println("请求执行成功[" +index+"]" + "    " + "请求标识: [" +  msg.getData() + "]");

		}
	}
	
	public boolean checkMsg(HQMsg msg,boolean isPre){
		return (msg.isOk() && msg.getVnum() == view 
				// pre阶段校验
		//（是前一个取反  ||  是主节点自己   ||          （ 视图的主节点等于消息的处理节点 且 消息的序号 等于 现在生成的序列号 ）
				&& (!isPre || msg.getNode() == index || (getPriNode(view) == msg.getNode() && msg.getNo() > genNo.get()))  );
	}	

	/**
	 * 检测超时情况
	 */
	private void checkHTimer() {
		//定时检查当前请求执行时长，超时更改共识方法，重新发送请求 
		//假定超时2000ms后，投票均已完成，未完成请求已失去完成可能
		if (curReq !=null && (System.currentTimeMillis() - curReq.getTime() >2000)) {
			curReq.setVnum(this.view);
			replyCount.set(0);
			if(curReq.getType() == HREQ) {
				curReq.setType(REQ);
				doSendCurReq();
			}else {
				curReq.setType(HREQ);
				doSendCurReq();
			}
		}
		
		List<String> remo = Lists.newArrayList();
		
		//检查消息超时
		for(Entry<String, Long> item : timeOuts.entrySet()){
			if(System.currentTimeMillis() - item.getValue() > 1000){
				// 超时还没达成一致，则本次投票无效
				//logger.info("投票无效["+index+"]:"+ item.getKey());
				remo.add(item.getKey());
			}
		}
		remo.forEach((it)->{
			remove(it);
		});
		
		remo.clear();
		
		//检查请求超时
		for(Entry<String, Long> item : timeOutsReq.entrySet()){
			if(System.currentTimeMillis() - item.getValue() > 600){
				// 请求超时
				remo.add(item.getKey());
			}
		}
		remo.forEach((data)->{
			//logger.info("请求主节点超时["+index+"]:"+data);
			timeOutsReq.remove(data);
			if(curReq != null && curReq.getData().equals(data)){
				// 作为客户端发起节点
				vnumAggreCount.incrementAndGet(this.view+1);
				votes_vnum.add(index+"@"+(this.view+1));
				HQMain.publish(curReq);
			}else{
				if(!this.viewOk) return; // 已经开始选举视图，不用重复发起
				this.viewOk = false;
				// 作为副本节点，广播视图变更投票
				HQMsg cv = new HQMsg(CV, this.index);
				cv.setVnum(this.view+1);
				HQMain.publish(cv);
			}			
		});	
		
		remo.clear();
		
		//检查Back阶段所有确认消息超时
		for(Entry<String, Long> item : timeOutsBack.entrySet()){
			if(System.currentTimeMillis() - item.getValue() > 600){
				// 请求超时
				remo.add(item.getKey());
			}
		}

		remo.forEach((data)->{
			//待完善，检查、找出缺失节点号、扣分、向pbft转发
			timeOutsBack.remove(data);
			timeOutsBackList.remove(data);

			HQMain.consensusNodes.forEach((node)->{
				if(timeOutsBackList.get(data).contains(node.getIndex())) {
					node.increCredit();
				}else{
					node.decreCredit();
				};
			});
		});
		
		
	}
		
	private void onChangeView(HQMsg msg) {
		// 收集视图变更
		String vkey = msg.getNode()+"@"+msg.getVnum();
		if(votes_vnum.contains(vkey)) return;
		votes_vnum.add(vkey);
		long count = vnumAggreCount.incrementAndGet(msg.getVnum());
		if(count >= 2*maxf+1){
			vnumAggreCount.clear();
			this.view = msg.getVnum();
			viewOk = true;
			logger.info("视图变更完成["+index+"]："+ view);
			// 重新发送请求
			if(curReq != null){
				curReq.setVnum(this.view);
				logger.info("请求重传["+index+"]："+ curReq);
				doSendCurReq();
			}
		}
	}
	
	private void onGetView(HQMsg msg) {
		if(msg.getData() == null){
			// 请求
			HQMsg sed = new HQMsg(msg);
			sed.setNode(index);
			sed.setVnum(view);
			sed.setData("initview");
			
			HQMain.send(msg.getNode(), sed);
		}else{
			// 响应
			if(this.viewOk)return;
			long count = vnumAggreCount.incrementAndGet(msg.getVnum());
			if(count >= 2*maxf+1){
				vnumAggreCount.clear();
				this.view = msg.getVnum();
				viewOk = true;
				//logger.info("视图初始化完成["+index+"]："+ view);
			}
		}		
	}
	
	/**
	 * 初始化视图view
	 */
	public void pubView(){
		HQMsg sed = new HQMsg(VIEW,index);
		HQMain.HQpublish(sed);
	}

	public int getView() {
		return view;
	}

	public void setView(int view) {
		this.view = view;
	}	
	
	public static void setHQSize(int Size) {
		HQSize = Size;
	}

	// 清理相关状态
	private void remove(String it) {
		votes_pre.remove(it);
		votes_pare.removeIf((vp)->{
			return StringUtils.startsWith(vp, it);
		});
		votes_comm.removeIf((vp)->{
			return StringUtils.startsWith(vp, it);
		});
		aggre_pare.remove(it);
		aggre_comm.remove(it);
		timeOuts.remove(it);
	}	
	
	public int getPriNode(int view){
		return view%size;
	}
	
	public void push(HQMsg msg){
		try {
			this.qbm.put(msg); 	//放入自身结构中的消息队列
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		logger.info("宕机[" +index+"]--------------");
		this.isRun = false;
	}
	
	public void setByzt() {
		logger.info("拜占庭[" +index+"]--------------");
		this.isByzt = true;
	}

	public int getIndex(){
		return this.index;
	}

	public void back() {
		logger.info("恢复[" +index+"]--------------");
		this.isRun = true;
	}
	
	public int getCredit() {
		return this.credit;
	}
	
	public void increCredit() {
		this.credit += 10; 
		
	}
	
	public void decreCredit() {
		this.credit -= 10;
	}

	public boolean isHQ() {
		return isHQ;
	}
	
	public void setIsSleep(boolean isSleep) {
		this.isSleep = isSleep;
	}

	public void setHQ(boolean isHQ) {
		this.isHQ = isHQ;
	}

// Implement comparable interface
    @Override
    public int compareTo(HQ o) {
       return this.credit - o.credit;
    }
}
