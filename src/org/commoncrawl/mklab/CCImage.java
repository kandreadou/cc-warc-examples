package org.commoncrawl.mklab;

/**
 * Created by kandreadou on 9/5/14.
 */
public class CCImage {

    public String src;
    public String alt;
    public String width;
    public String height;
    public String pageUrl;
    public String parentTxt;

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getAlt() {
        return alt;
    }

    public void setAlt(String alt) {
        this.alt = alt;
    }

    public String getWidth() {
        return width;
    }

    public void setWidth(String width) {
        this.width = width;
    }

    public String getHeight() {
        return height;
    }

    public void setHeight(String height) {
        this.height = height;
    }

    public String getPageUrl() {
        return pageUrl;
    }

    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }

    public String getParentTxt() {
        return parentTxt;
    }

    public void setParentTxt(String parentTxt) {
        this.parentTxt = parentTxt;
    }
}
