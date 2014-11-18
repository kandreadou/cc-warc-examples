package org.commoncrawl.mklab.analysis;

import org.mongodb.morphia.annotations.Id;

/**
 * Created by kandreadou on 11/18/14.
 */
public class CrawledImage {

    /**
     * {"src":"\n\t\t\t\t\t\t\t\thttp://s7ondemand1.scene7.com/is/image/MoosejawMB/10211640x1073912_zm?$thumb150$\n\t\t\t\t\t\t\t\t",
     * "alt":"ExOfficio Women\u0027s Crossback Diamond Dress",
     * "w":"",
     * "h":"",
     * "pageUrl":"http://www.moosejaw.com/moosejaw/shop/product_Isis-Women-s-Aida-Dress_10227940_10208_10000001_-1_",
     * "parentTxt":"ExOfficio Women\u0027s Crossback Diamond Dress",
     * "domSib":4,
     * "domDepth":23,
     * "domElem":"img"},
     *
     * @return
     */

    @Id
    public String id;
    public String src;
    public String normalizedSrc;
    public String alt, pageUrl, parentTxt, domElem;
    public int domSib, domDepth;
}
