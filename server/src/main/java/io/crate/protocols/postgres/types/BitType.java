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

package io.crate.protocols.postgres.types;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import io.crate.sql.tree.BitString;
import io.netty.buffer.ByteBuf;

public class BitType extends PGType<BitString> {

    public static final int OID = 1560;
    public static final BitType INSTANCE = new BitType();
    private int length;

    public BitType() {
        super(OID, -1, -1, "bit");
    }

    public BitType(int length) {
        this();
        this.length = length;
    }

    @Override
    public int typArray() {
        return PGArray.BIT_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.BIT_STRING.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, BitString value) {
        // TODO: verify this is the right format
        byte[] byteArray = value.bitSet().toByteArray();
        buffer.writeBytes(byteArray);
        return byteArray.length;
    }

    @Override
    public BitString readBinaryValue(ByteBuf buffer, int valueLength) {
        // TODO: verify this is the right format
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        return new BitString(BitSet.valueOf(bytes), length);
    }

    @Override
    byte[] encodeAsUTF8Text(BitString value) {
        assert length >= 0 : "BitType length must be set";
        return value.asBitString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    BitString decodeUTF8Text(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        return BitString.ofBitString(text);
    }
}
