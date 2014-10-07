package me.ralphya0.pre_processing;

public class Main {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        FieldsExtraction fe = new FieldsExtraction();
        String path = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\w_user_info_comp.csv";
        fe.extractAndOutput(path);
    }

}
