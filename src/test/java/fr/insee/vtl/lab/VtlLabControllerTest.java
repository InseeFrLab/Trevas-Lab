package fr.insee.vtl.lab;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class VtlLabControllerTest {

	@Autowired
	private MockMvc mvc;

	@Test
	public void testSqlSpec() throws Exception {

		String sqlQuery = "SELECT * from toto";
		JSONObject json = new JSONObject();
		json.put("type", "JDBC").put("url", "url").put("password", "password");
		json.put("query", sqlQuery).put("dbtype", "postgre");

		MockHttpServletRequestBuilder request = MockMvcRequestBuilders.post("/api/vtl/v3/sql-spec")
				.content(json.toString())
				.contentType(MediaType.APPLICATION_JSON);
		mvc.perform(request)
				.andExpect(status().isOk())
				.andExpect(content().string(sqlQuery));
	}
}
