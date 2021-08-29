package com.converter.mapper;

public class MapperClassFactory {
    public MapperClass createMapperClass(String mapperClass){
        switch(mapperClass.toLowerCase()){
            case("tsv"):
                return new TsvMapperClass();
            case("csv"):
                return new CsvMapperClass();
            case("json"):
                return new JsonMapperClass();
            default:
                return null;
        }
    }
}

class CsvMapperClass implements MapperClass{

    @Override
    public Class getMapperClass(){
        return CsvParser.class;
    }
}

class TsvMapperClass implements MapperClass{

    @Override
    public Class getMapperClass(){
        return TsvParser.class;
    }
}

class JsonMapperClass implements MapperClass{

    @Override
    public Class getMapperClass(){
        return JsonParser.class;
    }
}
