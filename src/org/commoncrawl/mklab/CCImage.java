package org.commoncrawl.mklab;

import com.google.gson.annotations.SerializedName;

/**
 * Created by kandreadou on 9/5/14.
 */
public class CCImage {

    @SerializedName("src")
    public String src;
    @SerializedName("alt")
    public String alt;
    @SerializedName("w")
    public String width;
    @SerializedName("h")
    public String height;
    @SerializedName("pageUrl")
    public String pageUrl;
    @SerializedName("parentTxt")
    public String parentTxt;
    @SerializedName("domSib")
    public int domSiblings;
    @SerializedName("domDepth")
    public int domDepth;


}
