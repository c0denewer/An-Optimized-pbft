package com.test;

import java.util.Map;
import org.apache.commons.lang3.RandomUtils;

import com.google.common.collect.Maps;

public class Test {
	
	public static Map<String,int[]> timeOutsBackList = Maps.newHashMap();

	public static void main(String arg[]) {

		System.out.println(RandomUtils.nextInt(10, 10));
	}
 
}