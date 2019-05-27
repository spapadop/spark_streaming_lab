import com.google.common.collect.Iterators;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.Status;

public class Exercise_2 {

    public static void get10MostPopularHashtagsInLast5min(JavaDStream<Status> statuses) {

        /**
         *  Get the stream of hashtags from the stream of tweets
         */

        JavaDStream<String> words = statuses
				.flatMap(tweet -> Iterators.forArray(
				        tweet.getText()
                                .toLowerCase()
                                .trim()
                                .replaceAll("\n", "")
                                .replaceAll("\t", "")
                                .split(" ")));

		JavaDStream<String> hashTags = words
				.filter(word -> word.startsWith("#"));

        //hashTags.print();

        /**
         *  Count the hashtags over a 5 minute window
         */

		JavaPairDStream<String, Integer> tuples = hashTags.
				mapToPair(hashtag -> new Tuple2<>(hashtag, 1));

		JavaPairDStream<String, Integer> counts = tuples
				.reduceByKeyAndWindow(
				        (i1, i2) -> i1 + i2,
                        (i1, i2) -> i1-i2,
                        Durations.minutes(5),
                        Durations.seconds(5));

        //counts.print();

        /**
         *  Find the top 10 hashtags based on their counts
         */

		JavaPairDStream<Integer, String> swappedCounts = counts
                .mapToPair(t -> t.swap());

		JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair( count -> count.sortByKey(false));

        sortedCounts.foreachRDD(rdd -> {
            String out = "\nTop 10 hashtags:\n";
            for (Tuple2<Integer, String> t : rdd.take(10)) {
                out = out + t.toString() + "\n";
            }
            System.out.println(out);
        });

    }

}