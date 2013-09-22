package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Max;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.*;
import cascading.pipe.assembly.*;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {
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
   * source field(s) : "id", "content"
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
    Pipe mainPipe = new Pipe("mainPipe");

    // sizeDPipe
    Pipe sizeDPipe = new Pipe("sizeDPipe", mainPipe);
    sizeDPipe = new Each(sizeDPipe, new Fields("id"), new DocSeqCreator(
        new Fields("seqNum")), Fields.ALL);
    sizeDPipe = new Discard(sizeDPipe, new Fields("content"));
    sizeDPipe = new GroupBy(sizeDPipe, new Fields("id"), new Fields("seqNum"),
        true);
    sizeDPipe = new Each(sizeDPipe, new Fields("seqNum"), new DocSizeDetector(
        new Fields("sizeD")), new Fields("id", "sizeD"));
    sizeDPipe = new Rename(sizeDPipe, new Fields("id"), new Fields("docId"));
    sizeDPipe = new Each(sizeDPipe, DebugLevel.VERBOSE, new Debug());

    // wordPipe
    Pipe wordPipe = new Pipe("wordPipe", mainPipe);
    wordPipe = new Each(wordPipe, new SplitDocumentFunction(new Fields("docId",
        "word")), Fields.RESULTS);

    // sizeDtPipe
    Pipe sizeDtPipe = new Pipe("sizeDtPipe", wordPipe);
    sizeDtPipe = new Unique(sizeDtPipe, new Fields("docId", "word"));
    sizeDtPipe = new CountBy(sizeDtPipe, new Fields("word"), new Fields(
        "sizeDt"));

    // freqPipe
    Pipe freqPipe = new Pipe("freqPipe", wordPipe);
    freqPipe = new CountBy(freqPipe, new Fields("docId", "word"), new Fields(
        "freq"));

    // maxFreqPipe
    Pipe maxFreqPipe = new Pipe("maxFreqPipe", freqPipe);
    maxFreqPipe = new GroupBy(maxFreqPipe, new Fields("docId"));
    maxFreqPipe = new Every(maxFreqPipe, new Fields("freq"), new Max(
        new Fields("maxFreq")), Fields.ALL);

    // mainPipe
    mainPipe = new CoGroup(freqPipe, new Fields("docId"), maxFreqPipe,
        new Fields("docId"), new Fields("docId1", "word", "freq", "docId2",
            "maxFreq"));
    mainPipe = new Discard(mainPipe, new Fields("docId2"));
    mainPipe = new Rename(mainPipe, new Fields("docId1"), new Fields("docId"));

    // tf(t,d) = f(t,d) / max (f(t',d))
    mainPipe = new Each(mainPipe, new Fields("freq", "maxFreq"),
        new TFCalculator(new Fields("tf")), Fields.ALL);

    // Merge sizeD to main pipe
    mainPipe = new CoGroup(mainPipe, new Fields("docId"), sizeDPipe,
        new Fields("docId"), new Fields("word", "freq", "maxFreq", "docId1",
            "tf", "sizeD", "docId2"));
    mainPipe = new Discard(mainPipe, new Fields("docId2"));
    mainPipe = new Rename(mainPipe, new Fields("docId1"), new Fields("docId"));

    // Merge sizeDt to main pipe
    mainPipe = new CoGroup(mainPipe, new Fields("word"), sizeDtPipe,
        new Fields("word"), new Fields("word1", "freq", "maxFreq", "tf",
            "sizeD", "docId", "word2", "sizeDt"));
    mainPipe = new Discard(mainPipe, new Fields("word2"));
    mainPipe = new Rename(mainPipe, new Fields("word1"), new Fields("word"));

    // idf(t, D) = log( size(D) / size(Dt) )
    mainPipe = new Each(mainPipe, new Fields("sizeD", "sizeDt"),
        new IDFCalculator(new Fields("idf")), Fields.ALL);

    // Calculate tfidf
    mainPipe = new Each(mainPipe, new Fields("tf", "idf"), new TFIDFCalculator(
        new Fields("tfidf")), Fields.ALL);
    mainPipe = new Retain(mainPipe, new Fields("docId", "tfidf", "word"));

    ExpressionFilter filter = new ExpressionFilter("tfidf < 0.1", Double.class);
    mainPipe = new Each(mainPipe, new Fields("tfidf"), filter);
    mainPipe = new GroupBy(mainPipe, new Fields("docId"), new Fields("docId",
        "tfidf", "word"), true);

    return FlowDef.flowDef().addSource(mainPipe, source)
        .addTailSink(mainPipe, sink).setDebugLevel(DebugLevel.VERBOSE);
  }
}
