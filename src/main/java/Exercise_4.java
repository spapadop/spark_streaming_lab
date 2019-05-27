import java.util.Arrays;
import java.util.List;


import com.google.common.collect.Iterators;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import twitter4j.Status;

public class Exercise_4 {

    public static void historicalAnalysis(JavaDStream<Status> statuses) {

        JavaDStream<String> words = statuses
                .flatMap(tweet -> Iterators.forArray(
                        tweet.getText()
                                .toLowerCase()
                                .trim()
                                .replaceAll("\n", "")
                                .split(" ")));

        JavaDStream<String> hashTags = words
                .filter(word -> word.startsWith("#"));

        JavaPairDStream<String, Integer> hashCounts = hashTags
                .mapToPair(hashtag -> new Tuple2<>(hashtag,1))
                .reduceByKey((count1,count2) -> count1+count2);

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    state.update(sum);
                    return output;
                };

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> counts = hashCounts
                .mapWithState(StateSpec.function(mappingFunc));


        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(count -> count.swap());
        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(count -> count.sortByKey(false));

        sortedCounts.cache();
        //As no window operation is being used, foreachRDD yields only 1 RDD for the current microbatch
        sortedCounts.foreachRDD(rdd -> {
            List<Tuple2<Integer,String>> sortedState = rdd.collect();
            try {
                System.out.println("Median: " + sortedState.get(sortedState.size() / 2));
            } catch (ArrayIndexOutOfBoundsException e) {
                if (sortedState.size() == 0){
                    System.out.println("No data to evaluate yet.");
                }
                else {
                    System.out.println(e.getMessage());
                }
            }
        });
        sortedCounts.foreachRDD(rdd -> System.out.println("Top 10: "+rdd.take(10)));

    }


}
