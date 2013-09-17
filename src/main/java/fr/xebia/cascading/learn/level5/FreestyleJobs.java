package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Max;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {
  public static class SplitLineFunction<Context> extends BaseOperation<Context>
      implements Function<Context> {
    private static final long serialVersionUID = 1L;

    public SplitLineFunction(Fields wordField) {
      super(1, wordField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
        FunctionCall<Context> functionCall) {
      String line = functionCall.getArguments().getString(0);
      String[] words = line.toLowerCase().split("[\\s,\\.'\\d\\[\\]\\(\\)/]+");
      for (String word : words) {
        if ("v".equalsIgnoreCase(word)) {
          continue;
        }
        functionCall.getOutputCollector().add(new Tuple(word));
      }
    }
  }

  /**
   * Word count is the Hadoop "Hello world" so it should be the first step.
   * 
   * source field(s) : "line"
   * sink field(s) : "word","count"
   */
  public static FlowDef countWordOccurences(Tap<?, ?, ?> source,
      Tap<?, ?, ?> sink) {
    Pipe pipe = new Pipe("countWordOccurences");

    pipe = new Each(pipe, new Fields("line"), new SplitLineFunction(new Fields(
        "word")), Fields.SWAP);

    //    String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";
    //    Function function = new RegexGenerator(new Fields("word"), regex);
    //    pipe = new Each(pipe, new Fields("line"), function);

    pipe = new CountBy(pipe, new Fields("word"), new Fields("count"));
    return FlowDef.flowDef().addSource(pipe, source).addTailSink(pipe, sink);
  }

  /**
   * Now, let's try a non trivial job : td-idf. Assume that each line is a
   * document.
   * 
   * source field(s) : "line"
   * sink field(s) : "docId","tfidf","word"
   * 
   * <pre>
   * t being a term
   * t' being any other term
   * d being a document
   * D being the set of documents
   * Dt being the set of documents containing the term t
   * 
   * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
   * 
   * where
   * 
   * tf(t,d) = f(t,d) / max (f(t',d))
   * ie the frequency of the term divided by the highest term frequency for the same document
   * 
   * idf(t, D) = log( size(D) / size(Dt) )
   * ie the logarithm of the number of documents divided by the number of documents containing the term t 
   * </pre>
   * 
   * Wikipedia provides the full explanation
   * @see http://en.wikipedia.org/wiki/tf-idf
   * 
   * If you are having issue applying functions, you might need to learn about field algebra
   * @see http://docs.cascading.org/cascading/1.2/userguide/html/ch03s04.html
   * 
   * {@link First} or {@link Max} can be useful for isolating the maximum.
   * 
   * {@link HashJoin} can allow to do cross join.
   * 
   * PS : Do no think about efficiency, at least, not for a first try.
   * PPS : You can remove results where tfidf < 0.1
   */
  public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
    return null;
  }

}
