/**
 * Copyright 2013 JakeCode Co., Ltd, all rights reserved.
 */
package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * @author jiakuanwang
 */
public class SplitLineFunction<Context> extends BaseOperation<Context>
    implements Function<Context> {
  private static final long serialVersionUID = 1L;

  public SplitLineFunction(Fields wordField) {
    super(1, wordField);
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {
    String line = functionCall.getArguments().getString(0);
    String[] words = line.toLowerCase().split("[\\s,\\.'\"\\d\\[\\]\\(\\)/]+");
    for (String word : words) {
      if ("v".equalsIgnoreCase(word) || "-".equalsIgnoreCase(word)) {
        continue;
      }
      functionCall.getOutputCollector().add(new Tuple(word));
    }
  }
}
