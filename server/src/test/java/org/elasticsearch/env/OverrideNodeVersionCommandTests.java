/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.env;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OverrideNodeVersionCommandTests extends ESTestCase {

    private Environment environment;
    private Path[] nodePaths;

    @Before
    public void createNodePaths() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        environment = TestEnvironment.newEnvironment(settings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            nodePaths = nodeEnvironment.nodeDataPaths();
        }
    }

    public void testFailsOnEmptyPath() {
        final Path emptyPath = createTempDir();
        final MockTerminal mockTerminal = new MockTerminal();
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, new Path[]{emptyPath}, environment));
        assertThat(elasticsearchException.getMessage(), equalTo(OverrideNodeVersionCommand.NO_METADATA_MESSAGE));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testFailsIfUnnecessary() throws WriteStateException {
        final Version nodeVersion = Version.fromId(between(Version.CURRENT.minimumIndexCompatibilityVersion().internalId, Version.CURRENT.internalId));
        NodeMetadata.FORMAT.writeAndCleanup(new NodeMetadata(randomAlphaOfLength(10), nodeVersion), nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, environment));
        assertThat(elasticsearchException.getMessage(), allOf(
            containsString("compatible with current version"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testWarnsIfTooOld() throws Exception {
        final String nodeId = randomAlphaOfLength(10);
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        NodeMetadata.FORMAT.writeAndCleanup(new NodeMetadata(nodeId, nodeVersion), nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput("n\n");
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, environment));
        assertThat(elasticsearchException.getMessage(), equalTo("aborted by user"));
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), nodePaths);
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testWarnsIfTooNew() throws Exception {
        final String nodeId = randomAlphaOfLength(10);
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        NodeMetadata.FORMAT.writeAndCleanup(new NodeMetadata(nodeId, nodeVersion), nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("yy", "Yy", "n", "yes", "true", "N", "no"));
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, environment));
        assertThat(elasticsearchException.getMessage(), equalTo("aborted by user"));
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), nodePaths);
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testOverwritesIfTooOld() throws Exception {
        final String nodeId = randomAlphaOfLength(10);
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        NodeMetadata.FORMAT.writeAndCleanup(new NodeMetadata(nodeId, nodeVersion), nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), nodePaths);
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
    }

    public void testOverwritesIfTooNew() throws Exception {
        final String nodeId = randomAlphaOfLength(10);
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        NodeMetadata.FORMAT.writeAndCleanup(new NodeMetadata(nodeId, nodeVersion), nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), nodePaths);
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
    }
}
