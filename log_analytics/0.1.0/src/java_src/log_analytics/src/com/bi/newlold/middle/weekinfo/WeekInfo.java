package com.bi.newlold.middle.weekinfo;

import java.util.List;



public class WeekInfo {

    /**
     * 
     * 
     * @Title: isArraysContainInList
     * @Description: 数组中的元素是否在链表内
     * @param @param dateIds
     * @param @param dataList
     * @param @return 参数说明
     * @return boolean 返回类型说明
     * @throws
     */
    public static boolean isArraysContainInList(Integer[] dateIds,
            List<Integer> dataList) {
        for (int i = 1; i < dateIds.length; i++) {
            if (dataList.contains(dateIds[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
