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

import com.google.auto.value.AutoValue;
import dev.herraiz.protos.Events.MyDummyEvent;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class SortWithWindows {
    @AutoValue
    public abstract static class Transform
            extends PTransform<
                    PCollection<KV<String, MyDummyEvent>>,
                    PCollection<KV<String, Iterable<MyDummyEvent>>>> {

        public static Transform withSessionDuration(int duration) {
            return new AutoValue_SortWithWindows_Transform.Builder().sessionGap(duration).build();
        }

        public abstract int sessionGap();

        @Override
        public PCollection<KV<String, Iterable<MyDummyEvent>>> expand(
                PCollection<KV<String, MyDummyEvent>> input) {

            // Add timestamps as sorting key
            PCollection<KV<String, KV<Long, MyDummyEvent>>> secondaryKey =
                    input.apply(
                            "Add secondary key",
                            ParDo.of(
                                    new DoFn<
                                            KV<String, MyDummyEvent>,
                                            KV<String, KV<Long, MyDummyEvent>>>() {
                                        @ProcessElement
                                        public void processElement(
                                                @Element KV<String, MyDummyEvent> element,
                                                OutputReceiver<KV<String, KV<Long, MyDummyEvent>>>
                                                        receiver) {
                                            receiver.output(
                                                    KV.of(
                                                            element.getKey(),
                                                            KV.of(
                                                                    element.getValue()
                                                                            .getEventTimestamp(),
                                                                    element.getValue())));
                                        }
                                    }));

            //            PCollection<KV<String, MyDummyEvent>> windowed =
            //                    input.apply(
            //                            "Session windowing",
            //                            Window.<KV<String, MyDummyEvent>>into(
            //
            // Sessions.withGapDuration(Duration.standardSeconds(30)))
            //                                    .triggering(AfterWatermark.pastEndOfWindow())
            //                                    .discardingFiredPanes()
            //                                    .withAllowedLateness(Duration.ZERO));

//            PCollection<KV<String, KV<Long, MyDummyEvent>>> windowed =
//                    secondaryKey.apply(
//                            "Session windowing",
//                            Window.<KV<String, KV<Long, MyDummyEvent>>>into(
//                                            Sessions.withGapDuration(Duration.standardSeconds(30)))
//                                    .triggering(AfterWatermark.pastEndOfWindow())
//                                    .discardingFiredPanes()
//                                    .withAllowedLateness(Duration.ZERO));

            PCollection<KV<String, KV<Long, MyDummyEvent>>> windowed =
                    secondaryKey.apply(
                            "Session windowing",
                            Window.<KV<String, KV<Long, MyDummyEvent>>>into(
                                            FixedWindows.of(Duration.standardSeconds(600)))
                                    .triggering(AfterWatermark.pastEndOfWindow())
                                    .discardingFiredPanes()
                                    .withAllowedLateness(Duration.ZERO));

            //            PCollection<KV<String, Iterable<MyDummyEvent>>> grouped =
            //                    windowed.apply("Group by key", GroupByKey.create());

            PCollection<KV<String, Iterable<KV<Long, MyDummyEvent>>>> grouped =
                    windowed.apply("Group by key", GroupByKey.create());

            PCollection<KV<String, Iterable<KV<Long, MyDummyEvent>>>> sorted =
                    grouped.apply(
                            "Sort",
                            SortValues.<String, Long, MyDummyEvent>create(
                                    BufferedExternalSorter.options()));

            //            return grouped.apply("Drop secondary key", ParDo.of(new
            // SortWithWindowsDoFn()));
            return grouped.apply("Drop secondary key", ParDo.of(new DropSecondaryKeyDoFn()));
        }

        @AutoValue.Builder
        public abstract static class Builder {
            public abstract Builder sessionGap(int d);

            public abstract Transform build();
        }
    }

    //    private static class SortWithWindowsDoFn
    //            extends DoFn<KV<String, Iterable<MyDummyEvent>>, KV<String,
    // Iterable<MyDummyEvent>>> {
    //        @ProcessElement
    //        public void processElement(
    //                @Element KV<String, Iterable<MyDummyEvent>> element,
    //                OutputReceiver<KV<String, Iterable<MyDummyEvent>>> receiver) {
    //
    //            List<MyDummyEvent> events = Lists.newArrayList(element.getValue());
    //            events.sort(new Events.MyDummyEventComparator());
    //
    //            receiver.output(KV.of(element.getKey(), events));
    //        }
    //    }

    private static class DropSecondaryKeyDoFn
            extends DoFn<
                    KV<String, Iterable<KV<Long, MyDummyEvent>>>,
                    KV<String, Iterable<MyDummyEvent>>> {
        @ProcessElement
        public void processElement(
                @Element KV<String, Iterable<KV<Long, MyDummyEvent>>> element,
                OutputReceiver<KV<String, Iterable<MyDummyEvent>>> receiver) {

            List<MyDummyEvent> sorted_events =
                    StreamSupport.stream(element.getValue().spliterator(), false)
                            .map(kv -> kv.getValue())
                            .collect(Collectors.toList());

            receiver.output(KV.of(element.getKey(), sorted_events));
        }
    }
}
