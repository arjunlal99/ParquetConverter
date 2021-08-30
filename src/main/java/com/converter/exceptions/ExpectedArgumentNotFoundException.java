package com.converter.exceptions;

public class ExpectedArgumentNotFoundException extends Exception{

    public ExpectedArgumentNotFoundException(){

    }

    public ExpectedArgumentNotFoundException(String msg){
        super(msg);
    }

}
