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
public class DocSizeDetector extends BaseOperation<DocSizeDetector.Context>
    implements Function<DocSizeDetector.Context> {
  private static final long serialVersionUID = 1L;

  public static class Context {
    int docSize = 0;
  }

  public DocSizeDetector(Fields docSizeFields) {
    super(1, docSizeFields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {
    int seqNum = functionCall.getArguments().getInteger("seqNum");

    Context context = functionCall.getContext();
    if (seqNum > context.docSize) {
      context.docSize = seqNum;
    }

    functionCall.getOutputCollector().add(new Tuple(context.docSize));
  }

}
