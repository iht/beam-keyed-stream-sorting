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
import dev.herraiz.beam.utils.Events;
import dev.herraiz.protos.Events.MyDummyEvent;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;

public class SortWithWindows {
    @AutoValue
    public abstract static class Transform
            extends PTransform<
                    PCollection<KV<String, MyDummyEvent>>,
                    PCollection<KV<String, Iterable<MyDummyEvent>>>> {

        public abstract int sessionGap();

        public static Transform withSessionDuration(int duration) {
            return new AutoValue_SortWithWindows_Transform.Builder().sessionGap(duration).build();
        }

        @AutoValue.Builder
        public abstract static class Builder {
            public abstract Builder sessionGap(int d);

            public abstract Transform build();
        }

        @Override
        public PCollection<KV<String, Iterable<MyDummyEvent>>> expand(
                PCollection<KV<String, MyDummyEvent>> input) {

            PCollection<KV<String, MyDummyEvent>> windowed =
                    input.apply(
                            "Session windowing",
                            Window.<KV<String, MyDummyEvent>>into(
                                            Sessions.withGapDuration(Duration.standardSeconds(30)))
                                    .triggering(AfterWatermark.pastEndOfWindow())
                                    .discardingFiredPanes()
                                    .withAllowedLateness(Duration.ZERO));

            PCollection<KV<String, Iterable<MyDummyEvent>>> grouped =
                    windowed.apply("Group by key", GroupByKey.create());

            return grouped.apply("Sort", ParDo.of(new SortWithWindowsDoFn()));
        }
    }

    private static class SortWithWindowsDoFn
            extends DoFn<KV<String, Iterable<MyDummyEvent>>, KV<String, Iterable<MyDummyEvent>>> {
        @ProcessElement
        public void processElement(
                @Element KV<String, Iterable<MyDummyEvent>> element,
                OutputReceiver<KV<String, Iterable<MyDummyEvent>>> receiver) {

            List<MyDummyEvent> events = Lists.newArrayList(element.getValue());
            events.sort(new Events.MyDummyEventComparator());

            receiver.output(KV.of(element.getKey(), events));
        }
    }
}
