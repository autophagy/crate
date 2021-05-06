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

package io.crate.execution.engine.collect.files;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.external.S3ClientHelper;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.JSON;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class S3FileInputTest extends ESTestCase {

    private static S3FileInput s3FileInput;
    private static List<S3ObjectSummary> listObjectSummaries;

    private static ObjectListing objectListing = mock(ObjectListing.class);
    private static S3ClientHelper clientBuilder = mock(S3ClientHelper.class);
    private static AmazonS3 amazonS3 = mock(AmazonS3.class);
    private static Predicate<URI> uriPredicate = mock(Predicate.class);

    private static final String BUCKET_NAME = "fakeBucket";
    private static final String PREFIX = "prefix";
    private static URI uri;


    @BeforeClass
    public static void setUpClass() throws Exception {
        uri = new URI("s3://fakeBucket/prefix");
        s3FileInput = new S3FileInput(clientBuilder);

        when(uriPredicate.test(any(URI.class))).thenReturn(true);
        when(amazonS3.listObjects(BUCKET_NAME, PREFIX)).thenReturn(objectListing);
        when(clientBuilder.client(uri)).thenReturn(amazonS3);
    }

    @Test
    public void testListListUrlsWhenEmptyKeysIsListed() throws Exception {
        S3ObjectSummary path = new S3ObjectSummary();
        path.setBucketName(BUCKET_NAME);
        path.setKey("prefix/");
        listObjectSummaries = objectSummaries();
        listObjectSummaries.add(path);

        when(objectListing.getObjectSummaries()).thenReturn(listObjectSummaries);

        List<URI> uris = s3FileInput.listUris(uri, uriPredicate);
        assertThat(uris.size(), is(2));
        assertThat(uris.get(0).toString(), is("s3://fakeBucket/prefix/test1.json.gz"));
        assertThat(uris.get(1).toString(), is("s3://fakeBucket/prefix/test2.json.gz"));
    }

    @Test
    public void testListListUrlsWithCorrectKeys() throws Exception {
        when(objectListing.getObjectSummaries()).thenReturn(objectSummaries());

        List<URI> uris = s3FileInput.listUris(uri, uriPredicate);
        assertThat(uris.size(), is(2));
        assertThat(uris.get(0).toString(), is("s3://fakeBucket/prefix/test1.json.gz"));
        assertThat(uris.get(1).toString(), is("s3://fakeBucket/prefix/test2.json.gz"));
    }

    private List<S3ObjectSummary> objectSummaries() {
        listObjectSummaries = new LinkedList<>();

        S3ObjectSummary firstObj = new S3ObjectSummary();
        S3ObjectSummary secondObj = new S3ObjectSummary();
        firstObj.setBucketName(BUCKET_NAME);
        secondObj.setBucketName(BUCKET_NAME);
        firstObj.setKey("prefix/test1.json.gz");
        secondObj.setKey("prefix/test2.json.gz");
        listObjectSummaries.add(firstObj);
        listObjectSummaries.add(secondObj);
        return listObjectSummaries;
    }

    @Test
    public void testArgsPassedToListObjectsWhenUriIncludesFileLevelWildCard() throws Exception {
        S3FileInputFactory factory = mock(S3FileInputFactory.class);
        when(factory.create()).thenReturn(s3FileInput);
        when(amazonS3.listObjects(any(String.class),any(String.class))).thenReturn(objectListing);
        when(clientBuilder.client(new URI("s3://fakeBucket3/prefix"))).thenReturn(amazonS3);
        createBatchIterator(
            List.of("s3://fakeBucket3/prefix/*.json"),
            Map.of(S3FileInputFactory.NAME, factory),
            JSON).moveNext();
        verify(amazonS3).listObjects("fakeBucket3","prefix");
    }

    @Test
    public void testArgsPassedToListObjectsWhenUriIncludesFolderLevelWildCard() throws Exception {
        S3FileInputFactory factory = mock(S3FileInputFactory.class);
        when(factory.create()).thenReturn(s3FileInput);
        when(amazonS3.listObjects(any(String.class),any(String.class))).thenReturn(objectListing);
        when(clientBuilder.client(new URI("s3://fakeBucket2"))).thenReturn(amazonS3);
        when(clientBuilder.client(new URI("s3://fakeBucket2/prefix"))).thenReturn(amazonS3);
        var it = createBatchIterator(
            List.of(
                "s3://fakeBucket2/prefix*/*.json",
                "s3://fakeBucket2/*/*.json",
                "s3://fakeBucket2/*/prefix2/prefix3/a.json",
                "s3://fakeBucket2/*/prefix2/*/*.json",
                "s3://fakeBucket2/prefix/p*x/*/*.json"
            ),
            Map.of(S3FileInputFactory.NAME, factory),
            JSON);
        while(it.moveNext());
        verify(amazonS3,times(4)).listObjects("fakeBucket2","");
        verify(amazonS3,times(1)).listObjects("fakeBucket2","prefix");
    }

    private BatchIterator<Row> createBatchIterator(Collection<String> fileUris,
                                                   Map<String, FileInputFactory> fileInputFactoryMap,
                                                   FileUriCollectPhase.InputFormat format) {

        NodeContext nodeCtx = new NodeContext(new Functions(Map.of()));
        InputFactory inputFactory = new InputFactory(nodeCtx);
        TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(txnCtx, FileLineReferenceResolver::getImplementation);

        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));
        return FileReadingIterator.newInstance(
            fileUris,
            inputs,
            ctx.expressions(),
            null,
            fileInputFactoryMap,
            false,
            1,
            0,
            CopyFromParserProperties.DEFAULT,
            format);
    }
}
