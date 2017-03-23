package com.mtty;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.impurity.Variance;
import org.apache.spark.mllib.tree.loss.LogLoss;
import org.apache.spark.mllib.tree.loss.Losses;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

import java.util.HashMap;
import java.util.Map;


public class GBDTTraining {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ctr");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 读取编码后的数据
        JavaRDD<LabeledPoint> trData = MLUtils.loadLibSVMFile(javaSparkContext.sc(), "hdfs://m1:8020/user/root/tr-svm").toJavaRDD();
        

        // 设置参数
        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
        boostingStrategy.setNumIterations(30);
        boostingStrategy.getTreeStrategy().setMaxDepth(7);
        boostingStrategy.setLoss(Losses.fromString("logLoss"));
        boostingStrategy.setLearningRate(1.0);

        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

        // 训练模型
        final GradientBoostedTreesModel model = GradientBoostedTrees.train(trData, boostingStrategy);

        DecisionTreeModel[] trees = model.trees();
        for(int i=0;i<trees.length;i++){
            System.out.println("trees "+i);
            System.out.println(trees[i].toDebugString());
        }

        // save model
        model.save(javaSparkContext.sc(), "hdfs://m1:8020/user/root/gbdt-model");

    }
}
