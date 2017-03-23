package com.mtty.tool;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShowTree {

    final static Pattern ifPattern = Pattern.compile("\\s+(?:If|Else)\\s+\\(\\w+\\s+(\\d+)\\s+<=\\s+(\\S+)\\)");
    final static Pattern predictPattern = Pattern.compile("\\s+Predict:\\s+(\\S+)\\s+(.+)");

    private static class Node{
        int id;
        int feature;
        double threashold;
        double gamma;
        String debugString;
        Node left;
        Node right;

        Node(){
            id = 0;
            feature = -1;
            threashold = 0.0;
            gamma = 0.0;
            left = null;
            right = null;
        }
    }

    static Node parseNode(String line){
        Matcher matcher = ifPattern.matcher(line);
        if(matcher.find()){
            Node n = new Node();
            n.feature = Integer.parseInt(matcher.group(1));
            n.threashold = Double.parseDouble(matcher.group(2));
            return n;
        }else {
            matcher = predictPattern.matcher(line);
            if(matcher.find()){
                Node n = new Node();
                n.feature = -1;
                n.gamma = Double.parseDouble(matcher.group(1));
                n.debugString = matcher.group(2);
                return n;
            }else{
                throw new RuntimeException("parse no node!!"+line);
            }
        }
    }

    static Node buildTree(ArrayList<String> lines,ArrayList<ArrayList<Integer>> levelIndices,int level,int begin,int end){
        ArrayList<Integer> edges = new ArrayList<>();
        ArrayList<Integer> indices = levelIndices.get(level);
        if(indices == null){
            throw new RuntimeException("indices of level "+level+" is null");
        }
        for(Integer idx:indices){
            if(idx>=begin&&idx<=end){
                edges.add(idx);
            }
        }
        if(edges.size()>2||edges.size()<=0){
            throw new RuntimeException("edges is invalid,count:"+edges.size());
        }
        String line = lines.get(edges.get(0));
        Node node = parseNode(line);
        if(node.feature != -1 && edges.size()==2) {
            node.left = buildTree(lines, levelIndices, level + 1, begin + 1, edges.get(1) - 1);
            node.right = buildTree(lines, levelIndices, level + 1, edges.get(1) + 1, end);
        }
        return node;
    }

    static String treeFromLines(ArrayList<String> lines){
        int[] levels = new int[lines.size()];
        int num = 0;
        int maxLevel = -1;
        for(String line:lines){
            byte[] bytes = line.getBytes();
            int levelNum = 0;
            while(levelNum<bytes.length&&bytes[levelNum]==' '){
                levelNum++;
            }
            levelNum-=2;
            if(levelNum<0){
                System.out.println("line invalid,line:"+line);
            }
            levels[num++] = levelNum;
            maxLevel = maxLevel<levelNum?levelNum:maxLevel;
        }
        ArrayList<ArrayList<Integer>> levelIndices = new ArrayList<>(maxLevel+1);
        for(int i=0;i<=maxLevel;i++){
            levelIndices.add(new ArrayList<>());
        }
        for(int i=0;i<levels.length;i++){
            if(levels[i]<0){
                continue;
            }
            ArrayList<Integer> indices  = levelIndices.get(levels[i]);
            indices.add(i);
        }
        Node root = buildTree(lines,levelIndices,0,0,levels.length-1);
        root.id = 1;
        Queue<Node> queue = new LinkedList<Node>();
        queue.add(root);
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        while(!queue.isEmpty()){
            Node n = queue.remove();
            if(n.id!=1){
                sb.append(",");
            }
            if(n.debugString==null) {
                sb.append("{\"id\":" + n.id + ",\"feature\":" + n.feature + ",\"threashold\":" + n.threashold + ",\"gamma\":" + n.gamma + "}");
            }else{
                sb.append("{\"id\":" + n.id + ",\"feature\":" + n.feature + ",\"threashold\":" + n.threashold + ",\"gamma\":" + n.gamma + "\"debugString\":\""
                        +n.debugString + "\"}");
            }
            if(n.left!=null){
                n.left.id = n.id<<1;
                queue.add(n.left);
            }
            if(n.right!=null){
                n.right.id = (n.id<<1)+1;
                queue.add(n.right);
            }
        }
        sb.append("]");
        return sb.toString();
    }

    static void buildReadableTree(String fileName,String outFileName){
        try {
            BufferedReader bf = new BufferedReader(new FileReader(fileName));
            BufferedWriter bw = new BufferedWriter(new FileWriter(outFileName));
            String line = null;
            HashMap<Integer,ArrayList<String>> treeLinesMap = new HashMap<>();
            int beginTreeNo = -1;
            while((line = bf.readLine())!=null){
                if(line.startsWith("trees")){//skip first two line
                    bf.readLine();
                    treeLinesMap.put(beginTreeNo++,new ArrayList<String>());
                }else if(!line.isEmpty()){
                    ArrayList<String> treeLines = treeLinesMap.get(beginTreeNo);
                    treeLines.add(line);
                }
            }
            bf.close();
            for(HashMap.Entry<Integer,ArrayList<String>> e:treeLinesMap.entrySet()){
                    ArrayList<String> treeLines = e.getValue();
                    String tree = treeFromLines(treeLines);
                    bw.write("tree"+e.getKey()+":"+tree+"\n");
            }
            bw.close();
        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        ShowTree.buildReadableTree("tree.log","tree.structure");
    }

}