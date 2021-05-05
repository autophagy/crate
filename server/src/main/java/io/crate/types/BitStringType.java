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

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;

public final class BitStringType extends DataType<BitSet> implements Streamer<BitSet>, FixedWidthType {

    public static final int ID = 25;
    // TODO: better max length limit, or use a `ZERO` sentinel?
    public static final BitStringType MAX = new BitStringType(8000);
    public static final String NAME = "bit";
    private final int length;

    public BitStringType(StreamInput in) throws IOException {
        this.length = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(length);
    }

    public BitStringType(int length) {
        this.length = length;
    }

    /**
     * number of bits
     **/
    public int length() {
        return length;
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        return List.of(DataTypes.INTEGER);
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature(getName(), List.of(TypeSignature.of(length)));
    }

    @Override
    public int compare(BitSet o1, BitSet o2) {
        if (o1.equals(o2)) {
            return 0;
        } else if (o1.size() < o2.size()) {
            return -1;
        } else {
            return 1;
        }
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.CUSTOM;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<BitSet> streamer() {
        return this;
    }

    @Override
    public BitSet sanitizeValue(Object value) {
        return (BitSet) value;
    }

    @Override
    public BitSet implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return (BitSet) value;
    }

    @Override
    public BitSet valueForInsert(Object value) {
        return super.valueForInsert(value);
    }

    @Override
    public BitSet readValueFrom(StreamInput in) throws IOException {
        return BitSet.valueOf(in.readByteArray());
    }

    @Override
    public void writeValueTo(StreamOutput out, BitSet v) throws IOException {
        out.writeByteArray(v.toByteArray());
    }

    @Override
    public int fixedSize() {
        return (int) Math.floor(length / 8.0);
    }
}
