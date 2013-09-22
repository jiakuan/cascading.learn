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
 * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
 *
 * @author jiakuanwang
 */
public class TFIDFCalculator<Context> extends BaseOperation<Context> implements
    Function<Context> {
  private static final long serialVersionUID = 1L;

  public TFIDFCalculator(Fields tfidfFields) {
    super(2, tfidfFields);
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {
    // tf-idf(t,d,D) = tf(t,d) * idf(t, D)
    Double tf = functionCall.getArguments().getDouble(0);
    Double idf = functionCall.getArguments().getDouble(1);

    Double tfidf = tf * idf;
    functionCall.getOutputCollector().add(new Tuple(tfidf));
  }
}
