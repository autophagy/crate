/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.scalar.array.ArraySummationFunctions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;

public class ArrayAvgFunction<T extends Number, R extends Number> extends Scalar<R, List<T>> {

    public static final String NAME = "array_avg";

    private final Function<List<T>, R> avgFunction;

    public static void register(ScalarFunctionModule module) {

        // All types except float and double have numeric average
        // https://www.postgresql.org/docs/13/functions-aggregate.html

        module.register(
            Signature.scalar(
                NAME,
                new ArrayType(DataTypes.NUMERIC).getTypeSignature(),
                DataTypes.NUMERIC.getTypeSignature()
            ),
            ArrayAvgFunction::new
        );

        for (var supportedType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            DataType inputDependantOutputType = DataTypes.NUMERIC;
            if (supportedType == DataTypes.FLOAT || supportedType == DataTypes.DOUBLE) {
                inputDependantOutputType = supportedType;
            }

            module.register(
                Signature.scalar(
                    NAME,
                    new ArrayType(supportedType).getTypeSignature(),
                    inputDependantOutputType.getTypeSignature()
                ),
                ArrayAvgFunction::new
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;

    private ArrayAvgFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;

        DataType<R> returnType = (DataType<R>) signature.getReturnType().createType();
        ArrayType<T> argumentType = (ArrayType<T>) signature.getArgumentTypes().get(0).createType();

        // It's safe to divide by size as empty array argument is checked before applying function in evaluate()
        // And for another case with array of only null values all ArraySummationFunctions return null and avgFunction returns null before division.

        if (argumentType.innerType() == DataTypes.FLOAT) {
            avgFunction = values -> {
                long size = values.stream().filter(Objects::nonNull).count();
                Float sum = (Float) ArraySummationFunctions.FLOAT.getFunction().apply(values);
                return sum == null ? null : returnType.implicitCast(sum / size);
            };
        } else if (argumentType.innerType() == DataTypes.DOUBLE) {
            avgFunction = values -> {
                long size = values.stream().filter(Objects::nonNull).count();
                Double sum = (Double) ArraySummationFunctions.DOUBLE.getFunction().apply(values);
                return sum == null ? null : returnType.implicitCast(sum / size);
            };
        } else if (argumentType.innerType() == DataTypes.NUMERIC) {
            avgFunction = values -> {
                long size = values.stream().filter(Objects::nonNull).count();
                BigDecimal sum = (BigDecimal) ArraySummationFunctions.NUMERIC.getFunction().apply(values);
                return sum == null ? null : returnType.implicitCast(sum.divide(BigDecimal.valueOf(size), MathContext.DECIMAL128));
            };
        } else {
            avgFunction = values -> {
                long size = values.stream().filter(Objects::nonNull).count();
                BigDecimal sum = (BigDecimal) ArraySummationFunctions.PRIMITIVE_NON_FLOAT_NOT_OVERFLOWING.getFunction().apply(values);
                return sum == null ? null : returnType.implicitCast(sum.divide(BigDecimal.valueOf(size), MathContext.DECIMAL128));
            };
        }

        ensureInnerTypeIsNotUndefined(boundSignature.getArgumentDataTypes(), signature.getName().name());
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public R evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        List<T> values = (List) args[0].value();
        if (values == null || values.isEmpty()) {
            return null;
        }
        return avgFunction.apply(values);
    }
}
