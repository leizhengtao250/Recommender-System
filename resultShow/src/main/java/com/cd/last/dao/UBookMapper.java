package com.cd.last.dao;

import com.cd.last.entity.UBook;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
public interface UBookMapper {
    List<UBook> findAllByUserid(String userid);
}