package org.apache.beam.fn.harness.data;

import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A wrapping FnDataReceiverWindowedValue<T> which counts the number of elements
 * consumed by the original nDataReceiverWindowedValue<T>.
 * @param <T> - The receiving type of the PTransform.
 */
public class ElementCountFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {

  private FnDataReceiver<WindowedValue<T>> original;
  private String pCollection;

  public ElementCountFnDataReceiver(
      FnDataReceiver<WindowedValue<T>> original, String pCollection) {
    this.original = original;
    this.pCollection = pCollection;
  }

  @Override
  public void accept(WindowedValue<T> input) throws Exception {
    String name = "ElementCount";
    String namespace = SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
    Metrics.counter(namespace, name).inc(input.getWindows().size());
    this.original.accept(input);
  }
}