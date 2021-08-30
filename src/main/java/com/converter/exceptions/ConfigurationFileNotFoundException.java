package com.converter.exceptions;

public class ConfigurationFileNotFoundException extends Exception{

    public ConfigurationFileNotFoundException(){

    }

    public ConfigurationFileNotFoundException(String msg){
        super(msg);
    }
}
