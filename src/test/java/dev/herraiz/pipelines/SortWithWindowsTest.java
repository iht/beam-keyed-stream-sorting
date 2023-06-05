/*
Copyright 2023 Google.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dev.herraiz.pipelines;

import static dev.herraiz.beam.data.Events.generateData;

import dev.herraiz.beam.transform.SortWithWindows;
import dev.herraiz.beam.utils.TestUtils;
import dev.herraiz.protos.Events.MyDummyEvent;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class SortWithWindowsTest {
    @Rule public TestPipeline pipeline = TestPipeline.create();

    // All messages will have the same key. The values themselves are not important
    private final String MSG_KEY = "my msg key";

    // The stream begins at this moment
    private final Instant TEST_EPOCH = Instant.parse("2017-02-03T10:37:30.00Z");

    // Number of events
    private final int NUM_EVENTS = 10;

    /** Test that the windowing approach produces sorted data */
    @Test
    public void testSortWithWindows() {
        // Data
        List<TimestampedValue<MyDummyEvent>> events = generateData(NUM_EVENTS, MSG_KEY, TEST_EPOCH);

        // Test stream
        TestStream.Builder<MyDummyEvent> streamBuilder =
                TestStream.create(ProtoCoder.of(MyDummyEvent.class));

        Instant currentWatermark = TEST_EPOCH;
        for (int k = 0; k < NUM_EVENTS; k++) {
            streamBuilder =
                    streamBuilder
                            .addElements(events.get(k))
                            .advanceProcessingTime(Duration.standardSeconds(2))
                            .advanceWatermarkTo(currentWatermark);
        }
        currentWatermark = currentWatermark.plus(Duration.standardSeconds(2));
        streamBuilder.advanceProcessingTime(Duration.standardSeconds(2));
        streamBuilder.advanceWatermarkTo(currentWatermark);
        TestStream<MyDummyEvent> stream = streamBuilder.advanceWatermarkToInfinity();

        // Pipeline
        PCollection<MyDummyEvent> eventsPColl = pipeline.apply(stream);
        PCollection<KV<String, MyDummyEvent>> keyedStream =
                eventsPColl
                        .apply("Add key", WithKeys.of(e -> e.getMsgKey()))
                        .setCoder(
                                KvCoder.of(
                                        AvroCoder.of(String.class),
                                        ProtoCoder.of(MyDummyEvent.class)));

        PCollection<KV<String, Iterable<MyDummyEvent>>> sorted =
                keyedStream.apply(
                        "Sort with window", SortWithWindows.Transform.withSessionDuration(30));

        PCollection<Iterable<MyDummyEvent>> keysDropped =
                sorted.apply(
                        "Drop keys",
                        MapElements.into(
                                        TypeDescriptors.iterables(
                                                TypeDescriptor.of(MyDummyEvent.class)))
                                .via(kv -> kv.getValue()));

        PCollection<Boolean> check = keysDropped.apply("Check", TestUtils.Check.isSorted());

        PAssert.thatSingleton("Windowed elements are sorted", check).isEqualTo(true);
        PAssert.thatSingletonIterable("Same elements as input", keysDropped)
                .containsInAnyOrder(
                        events.stream().map(ts -> ts.getValue()).collect(Collectors.toList()));

        pipeline.run();
    }
}
