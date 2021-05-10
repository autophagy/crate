/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.types;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderExtension;
import org.joda.time.Period;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import org.locationtech.spatial4j.shape.jts.JtsPoint;
import io.crate.data.RowN;
import io.crate.sql.tree.BitString;

import java.util.Base64;
import java.util.BitSet;
import java.util.Map;
import java.util.function.Function;

public class DataTypeXContentExtension implements XContentBuilderExtension {

    @Override
    public Map<Class<?>, XContentBuilder.Writer> getXContentWriters() {
        return Map.ofEntries(
            Map.entry(PointImpl.class, (b, v) -> {
                Point point = (Point) v;
                b.startArray();
                b.value(point.getX());
                b.value(point.getY());
                b.endArray();
            }),
            Map.entry(JtsPoint.class, (b, v) -> {
                Point point = (Point) v;
                b.startArray();
                b.value(point.getX());
                b.value(point.getY());
                b.endArray();
            }),
            Map.entry(Period.class, (b, v) -> {
                Period period = (Period) v;
                b.value(IntervalType.PERIOD_FORMATTER.print(period));
            }),
            Map.entry(RowN.class, (b, v) -> {
                RowN row = (RowN) v;
                b.startArray();
                for (int i = 0; i < row.numColumns(); i++) {
                    b.value(row.get(i));
                }
                b.endArray();
            }),
            Map.entry(TimeTZ.class, (b, v) -> {
                TimeTZ timetz = (TimeTZ) v;
                b.startArray();
                b.value(timetz.getMicrosFromMidnight());
                b.value(timetz.getSecondsFromUTC());
                b.endArray();
            }),
            Map.entry(BitString.class, (b, v) -> {
                // TODO: Go with different format?
                // This is both, stored in the _raw source and also what is returned via HTTP
                BitString bitString = (BitString) v;
                byte[] byteArray = bitString.bitSet().toByteArray();
                String encoded = Base64.getEncoder().withoutPadding().encodeToString(byteArray);
                b.value(encoded);
            })
        );
    }

    @Override
    public Map<Class<?>, XContentBuilder.HumanReadableTransformer> getXContentHumanReadableTransformers() {
        return Map.of();
    }

    @Override
    public Map<Class<?>, Function<Object, Object>> getDateTransformers() {
        return Map.of();
    }
}
