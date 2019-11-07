package com.cd.last.entity;

public class UBook {
    private Integer bookclass;

    private String userid;

    private String bookid;

    private String bookname;

    private String bookauthor;

    private String bookcover;

    public Integer getBookclass() {
        return bookclass;
    }

    public void setBookclass(Integer bookclass) {
        this.bookclass = bookclass;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid == null ? null : userid.trim();
    }

    public String getBookid() {
        return "images/covers/"+bookid+".jpg";
    }

    public void setBookid(String bookid) {
        this.bookid = bookid == null ? null : bookid.trim();
    }

    public String getBookname() {
        return bookname;
    }

    @Override
    public String toString() {
        return "UBook{" +
                "bookclass=" + bookclass +
                ", userid='" + userid + '\'' +
                ", bookid='" + bookid + '\'' +
                ", bookname='" + bookname + '\'' +
                ", bookauthor='" + bookauthor + '\'' +
                ", bookcover='" + bookcover + '\'' +
                '}';
    }

    public void setBookname(String bookname) {
        this.bookname = bookname == null ? null : bookname.trim();
    }

    public String getBookauthor() {
        return bookauthor;
    }

    public void setBookauthor(String bookauthor) {
        this.bookauthor = bookauthor == null ? null : bookauthor.trim();
    }

    public String getBookcover() {
        return bookcover;
    }

    public void setBookcover(String bookcover) {
        this.bookcover = bookcover == null ? null : bookcover.trim();
    }
}