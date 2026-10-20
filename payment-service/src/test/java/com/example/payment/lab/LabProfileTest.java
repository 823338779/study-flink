package com.example.payment.lab;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 默认运行方式不能意外暴露故障控制入口。 */
@WebMvcTest(controllers = LabController.class)
class LabProfileTest {
    @Autowired MockMvc mvc;

    @Test void labEndpointsAreAbsentWithoutLabProfile() throws Exception {
        mvc.perform(post("/api/lab/fail-after-commit").contentType("application/json")
                .content("{\"resultId\":\"A:1\"}")).andExpect(status().isNotFound());
        mvc.perform(post("/api/lab/results").contentType("application/json")
                .content("{}")).andExpect(status().isNotFound());
        mvc.perform(get("/api/lab/faults/A:1")).andExpect(status().isNotFound());
    }
}
