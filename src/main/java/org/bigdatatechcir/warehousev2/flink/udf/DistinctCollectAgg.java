package org.bigdatatechcir.warehousev2.flink.udf;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Set;

@FunctionHint(output = @DataTypeHint("STRING"))
public class DistinctCollectAgg extends UserDefinedFunction {


    @Override
    public FunctionKind getKind() {
        return null;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory dataTypeFactory) {
        return null;
    }

    @Override
    public Set<FunctionRequirement> getRequirements() {
        return super.getRequirements();
    }

    @Override
    public boolean isDeterministic() {
        return super.isDeterministic();
    }
}
