package org.commoncrawl.mklab.analysis;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import gr.forth.ics.memorymeasurer.MemoryMeasurer;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kandreadou on 10/31/14.
 */
public class Statistics {

    /**
     * A BloomFilter to store unique URLs.
     * Expected number of insertions: 15 billion
     * False positive probability: default 3%
     */
    public static BloomFilter<String> UNIQUE_URLS = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 15 * 1000 * 1000 * 1000);


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

    private static long  start = System.currentTimeMillis();

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

    public static Map<String, Multiset<String>> DOMAINS_FOR_IMAGES = new ConcurrentHashMap<String, Multiset<String>>();

    public static long GLOBAL_COUNT = 0;
    public static long DOMAIN_COUNT = 0;

    public static void addImageUrlForHost(String imageHost, String pageHost){
        if(DOMAINS_FOR_IMAGES.containsKey(pageHost)){
            DOMAINS_FOR_IMAGES.get(pageHost).add(imageHost);
        }else{
            Multiset<String> temp = ConcurrentHashMultiset.create();
            temp.add(imageHost);
            DOMAINS_FOR_IMAGES.put(pageHost, temp);
        }
    }

    public static void printStatistics() {

        try {
            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/home/kandreadou/Documents/statistics.txt",true)));

            int mb = 1024 * 1024;

            //Getting the runtime reference from system
            Runtime runtime = Runtime.getRuntime();

            writer.println("##### Heap utilization statistics [MB] #####");

            //Print used memory
            writer.println("Used Memory:"
                    + (runtime.totalMemory() - runtime.freeMemory()) / mb);

            //Print free memory
            writer.println("Free Memory:"
                    + runtime.freeMemory() / mb);

            //Print total available memory
            writer.println("Total Memory:" + runtime.totalMemory() / mb);

            //Print Maximum available memory
            writer.println("Max Memory:" + runtime.maxMemory() / mb);

            long memory = MemoryMeasurer.count(UNIQUE_URLS);

            writer.println("Bloom filter size:" + memory/mb);

            writer.println("WEBPAGES");
            Iterable<Multiset.Entry<String>> webPagesSetSortedByCount =
                    Multisets.copyHighestCountFirst(Statistics.NEWS_WEBPAGES_FREQUENCIES).entrySet();
            int newsWebPageCount = 0;
            for (Multiset.Entry<String> s : webPagesSetSortedByCount) {
                newsWebPageCount += s.getCount();
                writer.println( s.getElement() + " " + s.getCount());
            }
            writer.println("IMAGES");
            Iterable<Multiset.Entry<String>> imageSetSortedByCount =
                    Multisets.copyHighestCountFirst(NEWS_IMAGES_FREQUENCIES).entrySet();
            int newsImageCount = 0;
            for (Multiset.Entry<String> s : imageSetSortedByCount) {
                newsImageCount += s.getCount();
                writer.println(s.getElement() + " " + s.getCount());
            }
            writer.println("VIDEOS");
            Iterable<Multiset.Entry<String>> videoSetSortedByCount =
                    Multisets.copyHighestCountFirst(NEWS_VIDEO_FREQUENCIES).entrySet();
            int newsVideoCount = 0;
            for (Multiset.Entry<String> s : videoSetSortedByCount) {
                newsVideoCount += s.getCount();
                writer.println(s.getElement() + " " + s.getCount());
            }

            for (String host: DOMAINS_FOR_IMAGES.keySet()){
                writer.println("Image url distribution for host: " + host);
                Iterable<Multiset.Entry<String>> temp =
                        Multisets.copyHighestCountFirst(DOMAINS_FOR_IMAGES.get(host)).entrySet();
                for (Multiset.Entry<String> s : temp) {
                    writer.println(s.getElement() + " " + s.getCount());
                }
            }

            long duration = System.currentTimeMillis() - start;
            writer.println("UNIQUE URLS: " + Statistics.GLOBAL_COUNT);
            writer.println("UNIQUE DOMAINS: " + Statistics.DOMAIN_COUNT);
            writer.println("NEWS WEB PAGE COUNT: " + newsWebPageCount);
            writer.println("NEWS IMAGE COUNT: " + newsImageCount);
            writer.println("NEWS VIDEO COUNT: " + newsVideoCount);
            writer.println("Total time in millis: " + duration / 1000 + " seconds");

            writer.close();

        } catch (FileNotFoundException e) {
        } catch (UnsupportedEncodingException e) {
        } catch (IOException e) {
        }

    }
}
