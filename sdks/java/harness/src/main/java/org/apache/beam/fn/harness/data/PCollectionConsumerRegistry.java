package org.apache.beam.fn.harness.data;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

public class PCollectionConsumerRegistry {

  private ListMultimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers;

  public PCollectionConsumerRegistry() {
    pCollectionIdsToConsumers = ArrayListMultimap.create();
  }

  public <T> FnDataReceiver<WindowedValue<T>> registerAndWrap(
      String pCollectionId, FnDataReceiver<WindowedValue<T>> consumer) {
    FnDataReceiver<WindowedValue<T>> wrappedReceiver =
        new ElementCountFnDataReceiver<T>(consumer, pCollectionId);
    pCollectionIdsToConsumers.put(pCollectionId, (FnDataReceiver)wrappedReceiver);
    return wrappedReceiver;
  }

  public Set<String> keySet() {
    return pCollectionIdsToConsumers.keySet();
  }

  public FnDataReceiver<WindowedValue<?>> getOnlyElement(String pCollectionId) {
    return Iterables.getOnlyElement(pCollectionIdsToConsumers.get(pCollectionId));
  }

  public List<FnDataReceiver<WindowedValue<?>>> get(String pCollectionId) {
    return pCollectionIdsToConsumers.get(pCollectionId);
  }

}
