package com.mtty;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.loss.Loss;
import org.apache.spark.mllib.tree.loss.Losses;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;




public class GBDTValidating {

    private static void showModelTrees(JavaSparkContext javaSparkContext){
        GradientBoostedTreesModel model = GradientBoostedTreesModel.load(javaSparkContext.sc(),"hdfs://m1:8020/user/root/gbdt-model");
        DecisionTreeModel[] trees = model.trees();
        for(int i=0;i<trees.length;i++){
            System.out.println("trees "+i);
            System.out.println(trees[i].toDebugString());
        }
    }

    private static void calculateBias(JavaSparkContext javaSparkContext){
        // 读取编码后的数据
        JavaRDD<LabeledPoint> datas = MLUtils.loadLibSVMFile(javaSparkContext.sc(), "hdfs://m1:8020/user/root/tr-svm").toJavaRDD();
        double originLabelAvg = datas.aggregate(0.0,(v,point)->v+point.label(),(v1,v2)->v1+v2);
        originLabelAvg/=datas.count();
        double labelBias = Math.log1p(originLabelAvg)-Math.log1p(-originLabelAvg);
        System.out.println("originLabelAvg:"+originLabelAvg);
        System.out.println("labelBias:"+labelBias);
        Loss loss = Losses.fromString("logLoss");
        System.out.println("gradient for label 1:"+-loss.gradient(labelBias,1));
        System.out.println("gradient for label -1:"+-loss.gradient(labelBias,-1));
    }



    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ctr");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        calculateBias(javaSparkContext);
    }
}
