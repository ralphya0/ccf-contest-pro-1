package me.ralphya0.pre_processing;

import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws Throwable {
        // TODO Auto-generated method stub
        /*FieldsExtraction fe = new FieldsExtraction();
        String path = "F:\\work-space\\project-base\\ccf\\data\\result\\w_user_info_comp.csv";
        fe.extractAndOutput(path);
        ScoreComputing sc = new ScoreComputing();
        sc.fetching();
        
        InputGenerator ig = new InputGenerator();
        ig.inputFileGenerator("/home/ralphya0/ccf-competition/result/2014-10-18/word2vec_input.txt");
        
        NewsClassification nc = new NewsClassification();*/
        //NewsClassification nc = new NewsClassification("begin");
        AggregateByDate a = new AggregateByDate();
    }

}
