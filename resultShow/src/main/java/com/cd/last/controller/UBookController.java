package com.cd.last.controller;

import com.cd.last.entity.UBook;
import com.cd.last.service.IUBookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
public class UBookController {

    @Autowired
    private IUBookService iuBookService;

    @RequestMapping("/")
    public String hello(){
        return "index";
    }

    @RequestMapping("/login")
    public String check(Model model,String userid){
        System.out.println(userid);
       List<UBook> lUBook = iuBookService.getBook(userid);
        /**
         * 前面12个为轮播图战术
         */
        List<UBook> lunbolUBook;
        if(lUBook.size() > 12){
             lunbolUBook = lUBook.subList(0,12);
        }else{
             lunbolUBook =lUBook;
        }

       model.addAttribute("lunbolUBook",lunbolUBook);
        /**
         * 前面12个为轮播图战术
         */





       return "userShow";



    }


}
