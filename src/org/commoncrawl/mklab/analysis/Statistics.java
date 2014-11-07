package org.commoncrawl.mklab.analysis;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.net.URL;
import java.nio.charset.Charset;

/**
 * Created by kandreadou on 10/31/14.
 */
public class Statistics {

    /**
     * A BloomFilter to store unique URLs.
     * Expected number of insertions: 15 billion
     * False positive probability: default 3%
     */
    public static BloomFilter<String> UNIQUE_URLS;

    static {
        printMemoryStatistics();

        UNIQUE_URLS = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 15 * 1000 * 1000 * 1000);

        printMemoryStatistics();
    }


    /**
     * This fixed pool of bloom filters is a workaround because of the number of expected insertions.
     * BloomFilter has an upper limit of Integer.MAX_VALUE (~2.3 billion) and we expect around 20 billion.
     * There is an issue for this on the guava github https://github.com/google/guava/issues/1067
     * Proposed solutions is a fixed pool of bloom filters and hashing in order to select the proper bloom filter.
     * Using consecutive bloom filters is not recommended.
     * 1 billion per bloom filter
     */
    /*private static int NUM_BLOOM = 7;
    public static BloomFilter<String>[] UNIQUE_URLS = new BloomFilter[NUM_BLOOM];

    static {
        for (int i=0;i<UNIQUE_URLS.length; i++) {
            UNIQUE_URLS[i] = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 1500000000);
        }
    }
    public static boolean bloomURL(URL url){
        return UNIQUE_URLS.put(url.toString());
        /*int index = Math.abs(url.getHost().hashCode())%NUM_BLOOM;
        BloomFilter<String> bf = UNIQUE_URLS[index];
        return bf.put(url.toString());
        }
    */


    /**
     * A BloomFilter to store unique domains.
     * Expected number of insertions: 100 million
     * False positive probability: default 3%
     */
    public static BloomFilter<String> UNIQUE_DOMAINS = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 100000000);

    /**
     * A HashMap to store frequencies for image URLs from specific news domains
     */
    public static Multiset<String> NEWS_IMAGES_FREQUENCIES = ConcurrentHashMultiset.create();

    /**
     * A HashMap to store frequencies for video URLs from specific news domains
     */
    public static Multiset<String> NEWS_VIDEO_FREQUENCIES = ConcurrentHashMultiset.create();

    /**
     * A HashMap to store frequencies for webpage URLs from specific news domains
     */
    public static Multiset<String> NEWS_WEBPAGES_FREQUENCIES = ConcurrentHashMultiset.create();

    public static int GLOBAL_COUNT = 0;
    public static int DOMAIN_COUNT = 0;

    public static void printMemoryStatistics() {
        int mb = 1024 * 1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        System.out.println("##### Heap utilization statistics [MB] #####");

        //Print used memory
        System.out.println("Used Memory:"
                + (runtime.totalMemory() - runtime.freeMemory()) / mb);

        //Print free memory
        System.out.println("Free Memory:"
                + runtime.freeMemory() / mb);

        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);

        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
    }
}
