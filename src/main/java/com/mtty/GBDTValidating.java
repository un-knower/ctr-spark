package com.mtty;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.loss.Loss;
import org.apache.spark.mllib.tree.loss.Losses;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;


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
        JavaRDD<LabeledPoint> datas = MLUtils.loadLibSVMFile(javaSparkContext.sc(), "hdfs://m1:8020/user/root/tr-svm").toJavaRDD();
        datas = datas.map( oldPoint -> new LabeledPoint(oldPoint.label()*2 -1,oldPoint.features()));
        double originLabelAvg = datas.aggregate(0.0,(v,point)->v+point.label(),(v1,v2)->v1+v2);
        originLabelAvg/=datas.count();
        double labelBias = Math.log1p(originLabelAvg)-Math.log1p(-originLabelAvg);
        System.out.println("originLabelAvg:"+originLabelAvg);
        System.out.println("labelBias:"+labelBias);
        Loss loss = Losses.fromString("logLoss");
        System.out.println("gradient for label 1:"+-loss.gradient(labelBias,1));
        System.out.println("gradient for label -1:"+-loss.gradient(labelBias,-1));
    }


    private static void validateDataCtr(JavaSparkContext javaSparkContext){
        JavaRDD<LabeledPoint> datas = MLUtils.loadLibSVMFile(javaSparkContext.sc(), "hdfs://m1:8020/user/root/te-svm").toJavaRDD();
        GradientBoostedTreesModel model = GradientBoostedTreesModel.load(javaSparkContext.sc(),"hdfs://m1:8020/user/root/gbdt-model");
        JavaPairRDD<Double,Double> labelPairs = datas.mapToPair(point -> {
           Double ctr = 1.0/(1+Math.exp(-1.0*model.predict(point.features())));
            return new Tuple2<>(ctr,point.label()/1000000.0);
        });
        Tuple2<Double,Double> sums = labelPairs.aggregate(new Tuple2<Double,Double>(0.0,0.0),(t1,t2)->new Tuple2<Double, Double>(t1._1+t2._1,t1._2+t2._2),
                (t1,t2)->new Tuple2<Double, Double>(t1._1+t2._1,t1._2+t2._2));
        double labelAvg1= sums._1 / labelPairs.count();
        double labelAvg2= sums._2 / labelPairs.count();
        Tuple2<Double,Double> squareSums = labelPairs.map( t-> new Tuple2<>(Math.pow(t._1-labelAvg1,2),Math.pow(t._2-labelAvg2,2))).aggregate(new Tuple2<>(0.0,0.0),
                (t1,t2)->new Tuple2<Double, Double>(t1._1+t2._1,t1._2+t2._2),
                (t1,t2)->new Tuple2<Double, Double>(t1._1+t2._1,t1._2+t2._2));
        System.out.println("se1:"+(squareSums._1/labelPairs.count()));
        System.out.println("se2:"+(squareSums._2/labelPairs.count()));
    }

    private static void checkoutTrainDataOnModel(JavaSparkContext javaSparkContext){
        JavaRDD<LabeledPoint> datas = MLUtils.loadLibSVMFile(javaSparkContext.sc(), "hdfs://m1:8020/user/root/tr-svm").toJavaRDD();
        GradientBoostedTreesModel model = GradientBoostedTreesModel.load(javaSparkContext.sc(),"hdfs://m1:8020/user/root/gbdt-model");
        JavaPairRDD<Double,Double> labelPairs = datas.mapToPair(point -> {
            Double ctr = 1.0/(1+Math.exp(-1.0*model.predict(point.features())));
            return new Tuple2<>(ctr,point.label()/1000000.0);
        });
        labelPairs.saveAsTextFile("hdfs://m1:8020/user/root/trmodel.validate");
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ctr");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        calculateBias(javaSparkContext);
    }
}
