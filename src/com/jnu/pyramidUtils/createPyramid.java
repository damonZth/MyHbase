package com.jnu.pyramidUtils;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.util.ArrayList;
import java.util.List;

/**
 * 生成金字塔值的类
 *
 * 首先确定高维点v所在的金字塔i  ok了
 *
 * 然后确定v在的该金字塔中的高h
 */
public class createPyramid {

    public static void main(String[] args){
        List<Double> list = new ArrayList<>();
        list.add(0.6);
        list.add(0.4);
        list.add(0.8);
        list.add(0.1);

        System.out.println(getJMax(list));

    }




    /**
     * 获取金字塔值
     * @param list
     * @return
     */
    private static int getPyramidId(List<Double> list){
        int jMax = getJMax(list);
        int pyramid = 0;
        if(list.get(jMax) < 0.5){
            pyramid = jMax;
        }else{
            pyramid = jMax + list.size();
        }
        return pyramid;
    }

    /**
     * 获取Jmax
     * @param list
     * @return
     */
    private static int getJMax(List<Double> list){
        return getIndex(getList(list));
    }

    /**
     *  |x - 0.5|的绝对值
     * @param list
     * @return
     */
    private static List<Double> getList(List<Double> list){
        List<Double> reList = new ArrayList<>();
        for(Double d : list){
            reList.add(Math.abs(d - 0.5));
        }
        return reList;
    }

    /**
     * 获取List列表中,最大元素的位置
     * @param list
     * @return
     */
    private static int getIndex(List<Double> list){
        if(list.size() > 0){
            double max = list.get(0);
            int index = 0;
            for(int i = 0; i < list.size(); i ++){
                if(list.get(i) > max){
                    max = list.get(i);
                    index = i;
                }
            }
            return index;
        }
        return 0;
    }



}
