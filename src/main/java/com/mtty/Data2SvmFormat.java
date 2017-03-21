package com.mtty;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Data2SvmFormat {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("ctr");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<List<String>> oriRdd = javaSparkContext
                .textFile("hdfs://m1:8020/user/root/te.csv")
                .map(s -> Stream.of(s.split("[,]"))
                        .map(s1 -> {
                            if (s1 == null || s1.isEmpty()) {
                                return "";
                            } else {
                                return s1.trim();
                            }
                        }).collect(Collectors.toList()));

        Map<String,Double> teLabels = new HashMap<String,Double>();
        teLabels.putAll(javaSparkContext
                .textFile("hdfs://m1:8020/user/root/te_label.csv")
                .mapToPair(s->{
                    String[] strTuple = s.split(",");
                    double prob = Double.parseDouble(strTuple[1])*1000000.0;
                    if(prob<1){
                        prob = 0.0;
                    }
                    return new Tuple2<>(strTuple[0],prob);
                })
                .collectAsMap()
        );

        // csv header
        List<String> header = oriRdd.first();

        // 将每一行数据转换为map，key为header，value为值
        JavaRDD<Map<String, String>> datas = oriRdd
                .filter(l -> !l.get(0).equals(header.get(0)))
                .map(l -> {
                    Map<String, String> data = new HashMap<>();
                    for (int i = 0; i < header.size(); ++i) {
                        data.put(header.get(i), i >= l.size() ? "" : l.get(i));
                    }
                    data.put("Label",String.valueOf(teLabels.get(l.get(0))));
                    return data;
                });


        Map<String, Integer>[] targetCatFeats = new HashMap[26];

        for(int i=0;i<26;i++){
            JavaRDD<String> ss = javaSparkContext.textFile("hdfs://m1:8020/user/root/TargetCatFeats/C-" + i);
            if(ss.isEmpty()){
                continue;
            }
            targetCatFeats[i] = new HashMap<String,Integer>();
            targetCatFeats[i].putAll(ss.mapToPair(e -> {
                String[] strTuple = e.split(",");
                String[] key = strTuple[0].split("\\(");
                String[] value = strTuple[1].split("\\)");
                String k = key.length <= 1?"":key[1];
                Integer v = value.length == 1?Integer.parseInt(value[0]):0;
                return new Tuple2<>(k,v);
            }).collectAsMap());
        }

        //String[] fixedTargetCatFeats = new String[]{"C9-a73ee510", "C22-", "C17-e5ba7672", "C26-", "C23-32c7478e", "C6-7e0ccccf", "C14-b28479f6", "C19-21ddcdc9", "C14-07d13a8f", "C10-3b08e48b", "C6-fbad5c96", "C23-3a171ecb", "C20-b1252a9d", "C20-5840adea", "C6-fe6b92e5", "C20-a458ea53", "C14-1adce6ef", "C25-001f3601", "C22-ad3062eb", "C17-07c540c4", "C6-", "C23-423fab69", "C17-d4bb7bd8", "C2-38a947a1", "C25-e8b83407", "C9-7cc72ec2"};

        // 编码并保存为libsvm格式
        JavaRDD<LabeledVector> dataFeatures = datas.map(m -> {
            LabeledVector labeledVector = new LabeledVector();
            labeledVector.setLabel((int)Double.parseDouble(m.get("Label")));

            for (int i = 1; i < 14; ++i) {
                String field = m.get("I" + i);
                labeledVector.getFeatures().put(i, field != null && !field.isEmpty() ? Integer.parseInt(field) : -10);
            }

            for (int i = 1; i < 27; ++i) {
                String field = m.get("C" + i);
                if (field != null) {
                    Map<String, Integer> targetCatFeat = targetCatFeats[i - 1];
                    for(Map.Entry<String,Integer> e:targetCatFeat.entrySet()){
                        if(field.equals(e.getKey())){
                            labeledVector.getFeatures().put(13+e.getValue(),1);
                        }else{
                            labeledVector.getFeatures().put(13+e.getValue(),0);
                        }
                    }
                }
            }

//            for(int i=0;i<fixedTargetCatFeats.length;i++){
//                String category = fixedTargetCatFeats[i];
//                String[] sArr = category.split("-");
//                String field = m.get(sArr[0]);
//                if((sArr.length==2&& field != null && field.equals(sArr[1]))
//                        ||(sArr.length == 1 && field!=null && field.equals(""))){
//                    labeledVector.getFeatures().put(13+i+1,1);
//                }else{
//                    labeledVector.getFeatures().put(13+i+1,0);
//                }
//            }

            return labeledVector;
        });
        dataFeatures.saveAsTextFile("hdfs://m1:8020/user/root/te-svm");
    }

}
