package com.cd.last.service;

import com.cd.last.dao.UBookMapper;
import com.cd.last.entity.UBook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class UBookServiceImpl implements IUBookService {
    @Autowired
    private UBookMapper uBookMapper;
    @Override
    public List<UBook> getBook(String userid) {
        return uBookMapper.findAllByUserid(userid);
    }
}
