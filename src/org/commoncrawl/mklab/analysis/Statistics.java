package org.commoncrawl.mklab.analysis;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kandreadou on 10/31/14.
 */
public class Statistics {

    /**
     * A BloomFilter to store unique URLs.
     * Expected number of insertions: 500 million
     * False positive probability: default 3%
     */
    public static BloomFilter<String> UNIQUE_URLS = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")),500000000);

    /**
     * A BloomFilter to store unique domains.
     * Expected number of insertions: 100 million
     * False positive probability: default 3%
     */
    public static BloomFilter<String> UNIQUE_DOMAINS = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")),100000000);

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


}
