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

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.Base64;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;

import io.crate.sql.tree.BitString;

public class BitStringFieldMapper extends FieldMapper {

    private Integer length;

    protected BitStringFieldMapper(String simpleName,
                                   Integer position,
                                   Integer length,
                                   String defaultExpression,
                                   MappedFieldType fieldType,
                                   MappedFieldType defaultFieldType,
                                   Settings indexSettings,
                                   MultiFields multiFields,
                                   CopyTo copyTo) {
        super(
            simpleName,
            position,
            defaultExpression,
            fieldType,
            defaultFieldType,
            indexSettings,
            multiFields,
            copyTo
        );
        this.length = length;
    }

    public static final String CONTENT_TYPE = "bit";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new BitStringFieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.freeze();
        }
    }

    static class BitStringFieldType extends MappedFieldType {

        BitStringFieldType() {
            super();
        }

        BitStringFieldType(MappedFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new BitStringFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            BytesRef bytesRef = new BytesRef(((BitString) value).bitSet().toByteArray());
            return new TermQuery(new Term(name(), bytesRef));
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, BitStringFieldMapper> {

        private Integer length;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public BitStringFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new BitStringFieldMapper(
                name,
                position,
                length,
                defaultExpression,
                fieldType,
                defaultFieldType,
                context.indexSettings(),
                multiFieldsBuilder.build(this, context),
                copyTo);
        }

        public void length(Integer length) {
            this.length = length;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public org.elasticsearch.index.mapper.Mapper.Builder<?, ?> parse(
                String name,
                Map<String, Object> node,
                ParserContext parserContext) throws MapperParsingException {

            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            Object length = node.remove("length");
            assert length != null : "length property is required for `bit` type";
            builder.length((Integer) length);
            return builder;
        }
    }


    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        XContentParser parser = context.parser();
        String value = parser.textOrNull();
        if (value == null) {
            return;
        }
        byte[] bytes = Base64.getDecoder().decode(value);
        BytesRef binaryValue = new BytesRef(bytes);
        fields.add(new Field(fieldType().name(), binaryValue, fieldType()));
        fields.add(new SortedDocValuesField(fieldType().name(), binaryValue));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("length", length);
    }
}
