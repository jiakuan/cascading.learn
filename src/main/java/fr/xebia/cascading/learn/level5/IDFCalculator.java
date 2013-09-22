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
public class IDFCalculator<Context> extends BaseOperation<Context> implements
    Function<Context> {
  private static final long serialVersionUID = 1L;

  /**
   * idf(t, D) = log( size(D) / size(Dt) )
   *
   * @param idfFields
   */
  public IDFCalculator(Fields idfFields) {
    super(2, idfFields);
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {
    Double sizeD = functionCall.getArguments().getDouble(0);
    Double sizeDt = functionCall.getArguments().getDouble(1);

    Double idf = Math.log(sizeD / sizeDt);
    functionCall.getOutputCollector().add(new Tuple(idf));
  }
}
