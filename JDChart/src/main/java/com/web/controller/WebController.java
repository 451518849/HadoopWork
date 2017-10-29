package com.web.controller;

import com.mongodb.util.JSON;
import com.web.MongoDBJDBC;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/spark")
public class WebController {

    MongoDBJDBC mongoDBJDBC = new MongoDBJDBC();

    @RequestMapping("/wordCount")
    public @ResponseBody Map<String,Object> wordCount(){

        ArrayList list = mongoDBJDBC.getWordsPairFromCollection("adjwordcount");

        Map<String,Object> map = new HashMap<>();
        map.put("words",list);

        System.out.println(map);

        return map;
    }

    @RequestMapping("/monthCount")
    public @ResponseBody Map<String,Object> monthCount(){

        ArrayList list = mongoDBJDBC.getWordsPairFromCollection("monthwordcount");

        Map<String,Object> map = new HashMap<>();
        map.put("months",list);

        System.out.println(map);

        return map;
    }

    @RequestMapping("/barChart")
    public String barChart(HttpServletRequest request, HttpServletResponse response, Model model){

        return "barChart";
    }

    @RequestMapping("/time")
    public String timeCount() {

        return "timeChart";
    }


}
