package io.crate.expression.scalar;

import io.crate.execution.engine.aggregation.impl.KahanSummationForDouble;
import io.crate.execution.engine.aggregation.impl.KahanSummationForFloat;
import io.crate.expression.symbol.Literal;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static io.crate.testing.Asserts.assertThrows;

public class ArrayAvgFunctionTest extends ScalarTestCase {

    @Test
    public void test_array_returns_avg_of_elements() {

        // This test picks up random numbers but controls that overflow will not happen (overflow case is checked in another test).

        List<DataType> typesToTest = new ArrayList(DataTypes.NUMERIC_PRIMITIVE_TYPES);
        typesToTest.add(DataTypes.NUMERIC);

        for(DataType type: typesToTest) {
            var valuesToTest = TestingHelpers.getRandomsOfType(1, 10, type);

            if (type != DataTypes.FLOAT && type != DataTypes.DOUBLE && type != DataTypes.NUMERIC) {
                // check potential overflow and get rid of numbers causing overflow
                long sum = 0;
                for (int i = 0; i < valuesToTest.size(); i++) {
                    if (valuesToTest.get(i) != null) {
                        long nextNum = ((Number) valuesToTest.get(i)).longValue();
                        try {
                            sum = Math.addExact(sum, nextNum);
                        } catch (ArithmeticException e) {
                            valuesToTest = valuesToTest.subList(0, i); // excluding i
                            break;
                        }
                    }
                }
            }



            var kahanSummationForDouble = new KahanSummationForDouble();
            var kahanSummationForFloat = new KahanSummationForFloat();
            var optional = valuesToTest.stream()
                .filter(Objects::nonNull)
                .reduce((o1, o2) -> {
                    if(o1 instanceof BigDecimal) {
                        return ((BigDecimal) o1).add((BigDecimal) o2);
                    } else if(o1 instanceof Double) {
                        return kahanSummationForDouble.sum(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
                    } else if(o1 instanceof Float) {
                        return kahanSummationForFloat.sum(((Number) o1).floatValue(), ((Number) o2).floatValue());
                    } else {
                        return DataTypes.LONG.implicitCast(o1) + DataTypes.LONG.implicitCast(o2);
                    }
                });

            Number expected;
            if(optional.isPresent()) {
                var sum = optional.get();
                long size = valuesToTest.stream().filter(Objects::nonNull).count();
                if(sum instanceof BigDecimal) {
                    expected = ((BigDecimal) sum).divide(BigDecimal.valueOf(size), MathContext.DECIMAL128);
                } else if(sum instanceof Float) {
                    expected = (Float) sum / size;
                } else if(sum instanceof Double) {
                    expected = (Double) sum / size;
                } else {
                    BigDecimal bd = BigDecimal.valueOf(((Number) sum).longValue());
                    expected = bd.divide(BigDecimal.valueOf(size), MathContext.DECIMAL128);
                }

            } else {
                expected = null;
            }




            String expression = String.format(Locale.ENGLISH,"array_avg(?::%s[])", type.getName());
            assertEvaluate(expression, expected, Literal.of(valuesToTest, new ArrayType<>(type)));
        }
    }

    @Test
    public void test_array_avg_on_long_array_returns_numeric() {
        assertEvaluate("array_avg(long_array)",
            new BigDecimal(Long.MAX_VALUE),
            Literal.of(List.of(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE), new ArrayType<>(DataTypes.LONG))
        );
    }

    @Test
    public void test_array_avg_ignores_null_element_values() {
        assertEvaluate("array_avg([null, 1])", BigDecimal.ONE);
    }

    @Test
    public void test_all_elements_nulls_results_in_null() {
        assertEvaluate("array_avg([null, null]::integer[])", null);
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluate("array_avg(null::int[])", null);
    }

    @Test
    public void test_array_avg_returns_null_for_null_values() {
        assertEvaluate("array_avg(null)", null);
    }

    @Test
    public void test_empty_array_results_in_null() {
        assertEvaluate("array_avg(cast([] as array(integer)))", null);
    }

    @Test
    public void test_empty_array_given_directly_throws_exception() {
        assertThrows(() -> assertEvaluate("array_avg([])", null),
            UnsupportedOperationException.class,
            "Unknown function: array_avg([]), no overload found for matching argument types: (undefined_array).");
    }
}
