package com.nus.cool.core.io.writestore;

import java.io.DataOutput;
import java.io.IOException;

import com.nus.cool.core.schema.FieldType;


// whether to record the meta data of field in this chunk
// Through filter to speed up the cohort processing
public class DataInvariantHashFieldWS implements DataFieldWS{ 
    
    private FieldType fieldType;

    // private final MetaFieldWS metaFieldWS;

    public DataInvariantHashFieldWS(FieldType fieldType, MetaFieldWS metaField){
        this.fieldType = fieldType;
        // this.metaFieldWS = metaField;
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {
        return 0;
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public void put(String value) throws IOException {
        // // for invariant data field, no need to write data
        // int gId = this.metaFieldWS.find(value);
        // if (gId == -1){
        //     throw new IllegalArgumentException("Value not exist in dimension: " + tupleValue);
        // }
    }
    
}
