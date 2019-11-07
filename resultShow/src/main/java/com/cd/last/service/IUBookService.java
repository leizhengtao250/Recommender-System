package com.cd.last.service;

import com.cd.last.entity.UBook;

import java.util.List;

public interface IUBookService {
    List<UBook> getBook(String userid);
}
