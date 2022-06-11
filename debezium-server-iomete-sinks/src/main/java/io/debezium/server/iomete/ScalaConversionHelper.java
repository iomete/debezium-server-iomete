package io.debezium.server.iomete;

import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ScalaConversionHelper {

    public static <F, T> Seq<T> mapToSeq(List<F> list, Function<F, T> mapper) {
        List<T> transformedList = list.stream().map(mapper).collect(Collectors.toList());
        return listToSeq(transformedList);
    }

    public static <T> Seq<T> listToSeq(List<T> list) {
        return (Seq<T>) JavaConverters.asScalaIterator(list.iterator()).toSeq();
    }
}
