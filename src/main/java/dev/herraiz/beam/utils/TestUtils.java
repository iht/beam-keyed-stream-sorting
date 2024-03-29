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

package dev.herraiz.beam.utils;

import com.google.auto.value.AutoValue;
import dev.herraiz.protos.Events.MyDummyEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class TestUtils {

    @AutoValue
    public static class Check
            extends PTransform<PCollection<Iterable<MyDummyEvent>>, PCollection<Boolean>> {

        public static Check isSorted() {
            return new AutoValue_TestUtils_Check();
        }

        @Override
        public PCollection<Boolean> expand(PCollection<Iterable<MyDummyEvent>> input) {
            return input.apply("Check sorted", ParDo.of(new CheckSortedDoFn()));
        }
    }

    private static class CheckSortedDoFn extends DoFn<Iterable<MyDummyEvent>, Boolean> {
        @ProcessElement
        public void processElement(
                @Element Iterable<MyDummyEvent> events, OutputReceiver<Boolean> receiver) {
            boolean isSorted = true;
            MyDummyEvent previousEvent = null;
            for (MyDummyEvent event : events) {
                if (previousEvent == null) {
                    previousEvent = event;
                    continue;
                }

                if (event.getEventTimestamp() < previousEvent.getEventTimestamp()) {
                    isSorted = false;
                    break;
                }

                previousEvent = event;
            }

            receiver.output(isSorted);
        }
    }
}
