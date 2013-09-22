/**
 * Copyright 2013 JakeCode Co., Ltd, all rights reserved.
 */
package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * @author jiakuanwang
 */
public class DocSeqCreator extends BaseOperation<DocSeqCreator.Context>
    implements Function<DocSeqCreator.Context> {
  private static final long serialVersionUID = 1L;

  public static class Context {
    int seqNum = 0;
  }

  public DocSeqCreator(Fields seqNumFields) {
    super(1, seqNumFields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {

    Context context = functionCall.getContext();
    context.seqNum++;

    functionCall.getOutputCollector().add(new Tuple(context.seqNum));
  }


}
