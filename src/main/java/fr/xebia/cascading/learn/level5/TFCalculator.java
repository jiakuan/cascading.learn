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
public class TFCalculator<Context> extends BaseOperation<Context> implements
    Function<Context> {
  private static final long serialVersionUID = 1L;

  public TFCalculator(Fields tfFields) {
    super(2, tfFields);
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {
    Double freq = functionCall.getArguments().getDouble(0);
    Double maxFreq = functionCall.getArguments().getDouble(1);

    Double tf = freq / maxFreq;
    functionCall.getOutputCollector().add(new Tuple(tf));
  }
}
