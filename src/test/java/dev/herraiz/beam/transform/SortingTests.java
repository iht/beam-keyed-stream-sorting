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

package dev.herraiz.beam.transform;

import static dev.herraiz.beam.transform.CommonTestConfig.*;

import dev.herraiz.protos.Events;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TimestampedValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SortingTests {
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    private static final List<TimestampedValue<Events.MyDummyEvent>> events =
            getTimestampedValues(NUM_EVENTS);

    @Before
    public void testDummyIgnore() {

        // Warm up test with small set
        int smallSetSize = 10;
        pipeline =
                buildTestPipelineWithNumMessages(
                        "Windowed elements are sorted",
                        getTimestampedValues(smallSetSize),
                        pipeline,
                        SortWithWindows.Transform.withSessionDuration(30),
                        smallSetSize);

        pipeline.run();
    }

    @Test
    public void testSortWithAnnotations() {
        pipeline =
                buildTestPipeline(
                        "Annotation list is sorted",
                        events,
                        pipeline,
                        SortWithAnnotations.Transform.withSessionDuration(30));

        pipeline.run();
    }

    @Test
    public void testSortWithMapState() {
        pipeline =
                buildTestPipeline(
                        "Windowed elements are sorted",
                        events,
                        pipeline,
                        SortWithMapState.Transform.withSessionDuration(30));

        pipeline.run();
    }

    @Test
    public void testSortWithStateOrderedList() {
        pipeline =
                buildTestPipeline(
                        "State list is sorted",
                        events,
                        pipeline,
                        SortWithState.Transform.withSessionDuration(30));

        pipeline.run();
    }

    @Test
    public void testSortWithWindows() {
        pipeline =
                buildTestPipeline(
                        "Windowed elements are sorted",
                        events,
                        pipeline,
                        SortWithWindows.Transform.withSessionDuration(30));

        pipeline.run();
    }
}
