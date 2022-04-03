package com.pbft;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class Pbft {

	Logger logger = LoggerFactory.getLogger(getClass());
	
	public static final int CV = -2; // 视图变更
	public static final int VIEW = -1; // 请求视图
	public static final int REQ = 0; // 请求数据
	public static final int PP = 1; // 预准备阶段
	public static final int PA = 2; // 准备阶段
	public static final int CM = 3; // 提交阶段
	public static final int REPLY = 4; // 回复
		
	public int size; // 总节点数
	public int maxf; // 最大失效节点	
	private int index; // 节点标识
	private int isByz; //拜占庭节点标志
	
	private int view; // 视图view
	private volatile boolean viewOk = false; // 视图状态
	private volatile boolean isRun = false;
	

	// 消息队列
	private BlockingQueue<PbftMsg> qbm = Queues.newLinkedBlockingQueue();
	
	// 预准备阶段投票信息
	private Set<String> votes_pre = Sets.newConcurrentHashSet();	
	// 准备阶段投票信息
	private Set<String> votes_pare = Sets.newConcurrentHashSet();
	private AtomicLongMap<String> aggre_pare = AtomicLongMap.create();	
	// 提交阶段投票信息
	private Set<String> votes_comm = Sets.newConcurrentHashSet();
	private AtomicLongMap<String> aggre_comm = AtomicLongMap.create();
	
	// 成功处理过的请求
	private Map<String,PbftMsg> doneReq = Maps.newConcurrentMap();
	// 作为主节点受理过的请求
	private Map<String,PbftMsg> applyReq = Maps.newConcurrentMap();
	
	// 视图初始化 投票情况
	private AtomicLongMap<Integer> vnumAggreCount = AtomicLongMap.create();
	private Set<String> votes_vnum = Sets.newConcurrentHashSet();
	
	// 请求响应回复情况
	private AtomicLong replyCount = new AtomicLong();
	
	// 消息超时
	private Map<String,Long> timeOuts = Maps.newHashMap();	
	// 请求超时
	private Map<String,Long> timeOutsReq = Maps.newHashMap();
	// 请求队列
	private BlockingQueue<PbftMsg> reqQueue = Queues.newLinkedBlockingDeque(100);
	// 当前请求
	private PbftMsg curReq;
	
	private volatile AtomicInteger genNo = new AtomicInteger(0); // 序列号生成
	
	private Timer timer;
	
	public Pbft(int node,int size,double ratio) {
		this.index = node;
		this.size = size;
		this.maxf = (size-1)/3;
		this.isByz = RandomUtils.nextInt(0, (int)(ratio ==0 ? 0:(1/ratio)));
		timer = new Timer("timer"+node);
	}
	
	public Pbft start(){
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						//从消息队列中取出一个消息
						//logger.info("pbft run");
						PbftMsg msg = qbm.take();						
						doAction(msg);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}				
			}
		}).start();
		
		//logger.info("开始初始化");
		isRun = true;
		timer.schedule(new TimerTask() {
			int co = 0;
			@Override
			public void run() {
				if(co == 0){
					// 启动后先同步视图
					pubView();
				}
				co++;
				doReq();
				checkTimer();
			}
		}, 10, 100);
		return this;
	}	
	
	/**
	 * 产生请求
	 * @param data
	 * @throws InterruptedException 
	 */
	public void req(String data) throws InterruptedException{
		PbftMsg req = new PbftMsg(Pbft.REQ, this.index);
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
		PbftMain.send(getPriNode(view), curReq);
	}
	
	protected boolean doAction(PbftMsg msg) {
		//logger.info("do action");
		if(!isRun) return false;
		if(msg != null){
			//logger.info("收到消息[" +index+"]:"+ msg);
			switch (msg.getType()) {
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

	private void onReq(PbftMsg msg) {
		if(!msg.isOk()) return;
		PbftMsg sed = new PbftMsg(msg);
		sed.setNode(index);
		if(msg.getVnum() < view) return;
		if(msg.getVnum() == index){
			if(applyReq.containsKey(msg.getDataKey())) return; // 已经受理过
			applyReq.put(msg.getDataKey(), msg);
			// 主节点收到C的请求后进行广播
			sed.setType(PP);
			// 主节点生成序列号
			int no = genNo.incrementAndGet();
			sed.setNo(no);
			PbftMain.publish(sed);
		}else if(msg.getNode() != index){
			// 非主节点收到，说明可能主节点宕机
			if(doneReq.containsKey(msg.getDataKey())){
				// 已经处理过，直接回复
				sed.setType(REPLY);
				PbftMain.send(msg.getNode(), sed);
			}else{
				// 认为客户端进行了CV投票
				votes_vnum.add(msg.getNode()+"@"+(msg.getVnum()+1));
				vnumAggreCount.incrementAndGet(msg.getVnum()+1);
				// 未处理，说明可能主节点宕机，转发主节点试试
				//logger.info("转发主节点[" +index+"]:"+ msg);
				PbftMain.send(getPriNode(view), sed);
				timeOutsReq.put(msg.getData(), System.currentTimeMillis());
			}			
		}
	}	

	private void onPrePrepare(PbftMsg msg) {
		if(!checkMsg(msg,true)) {
			return;
		}
		
		String key = msg.getDataKey();
		if(votes_pre.contains(key))	return;
		
		votes_pre.add(key);
		// 启动超时控制
		timeOuts.put(key, System.currentTimeMillis());
		// 移除请求超时，假如有请求的话
		timeOutsReq.remove(msg.getData());
		// 进入准备阶段
		PbftMsg sed = new PbftMsg(msg);
		sed.setType(PA);
		sed.setNode(index);
		PbftMain.publish(sed);
	}
	

	private void onPrepare(PbftMsg msg) {
		if(!checkMsg(msg,false)) {
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
			PbftMsg sed = new PbftMsg(msg);
			sed.setType(CM);
			sed.setNode(index);
			doneReq.put(sed.getDataKey(), sed);
			PbftMain.publish(sed);
		}
		// 后续的票数肯定凑不满，超时自动清除			
	}

	private void onCommit(PbftMsg msg) {
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
				PbftMsg sed = new PbftMsg(msg);
				sed.setType(REPLY);
				sed.setNode(index);
				// 回复客户端
				PbftMain.send(sed.getOnode(), sed);
			}			
		}
	}
	
	private void onReply(PbftMsg msg) {
		if(curReq == null || !curReq.getData().equals(msg.getData()))return;
		long count = replyCount.incrementAndGet();
		if(count >= maxf+1){
			//logger.info("消息确认成功[" +index+"]:"+ msg);
			replyCount.set(0);
			curReq = null; // 当前请求已经完成
			// 执行相关逻辑					
			PbftMain.collectTimes(msg.computCostTime());
			//logger.info("请求执行成功[" +index+"]:"+msg);
			
		}
	}

	private void onGetView(PbftMsg msg) {
		if(msg.getData() == null){
			// 请求
			PbftMsg sed = new PbftMsg(msg);
			sed.setNode(index);
			sed.setVnum(view);
			sed.setData("initview");
			
			PbftMain.send(msg.getNode(), sed);
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
	
	private void onChangeView(PbftMsg msg) {
		// 收集视图变更
		String vkey = msg.getNode()+"@"+msg.getVnum();
		if(votes_vnum.contains(vkey)){
			return;
		}
		votes_vnum.add(vkey);
		long count = vnumAggreCount.incrementAndGet(msg.getVnum());
		if(count >= 2*maxf+1){
			vnumAggreCount.clear();
			this.view = msg.getVnum();
			viewOk = true;
			//logger.info("视图变更完成["+index+"]："+ view);
			// 可以继续发请求
			if(curReq != null){
				curReq.setVnum(this.view);
				//logger.info("请求重传["+index+"]："+ curReq);
				doSendCurReq();
			}
		}
	}
	
	public boolean checkMsg(PbftMsg msg,boolean isPre){
		return (msg.isOk() && msg.getVnum() == view 
				// pre阶段校验
				&& (!isPre || msg.getNode() == index || (getPriNode(view) == msg.getNode() && msg.getNo() > genNo.get())));
	}
	
	/**
	 * 检测超时情况
	 */
	private void checkTimer() {
		//定时检查当前请求执行时长，超时重新发送请求 
		//假定超时10000ms后，投票均已完成，未完成请求已失去完成可能
		if (curReq !=null && (System.currentTimeMillis() - curReq.getTime() >10000)) {
			curReq.setVnum(this.view);
			replyCount.set(0);
			doSendCurReq();
		}		
			
		List<String> remo = Lists.newArrayList();
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
				PbftMain.publish(curReq);
			}else{
				if(!this.viewOk) return; // 已经开始选举视图，不用重复发起
				this.viewOk = false;
				// 作为副本节点，广播视图变更投票
				PbftMsg cv = new PbftMsg(CV, this.index);
				cv.setVnum(this.view+1);
				PbftMain.publish(cv);
			}			
		});		
	}
	
	public int getPriNode(int view){
		return view%size;
	}
	
	public void push(PbftMsg msg){
		try {
			this.qbm.put(msg);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// 清理请求相关状态
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
	
	/**
	 * 广播视图view
	 */
	public void pubView(){
		PbftMsg sed = new PbftMsg(VIEW,index);
		PbftMain.publish(sed);
	}

	public int getView() {
		return view;
	}

	public void setView(int view) {
		this.view = view;
	}
	
	public void close(){
		//logger.info("宕机[" +index+"]--------------");
		this.isRun = false;
	}

	public int getIndex(){
		return this.index;
	}

	public void back() {
		//logger.info("恢复[" +index+"]--------------");
		this.isRun = true;
	}
}
