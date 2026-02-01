package com.apilytics.core.openapi

import munit.FunSuite

class ParserSuite extends FunSuite {

  private val simpleSpec =
    """{
      |  "openapi": "3.0.0",
      |  "info": { "title": "Test", "version": "1.0" },
      |  "servers": [{ "url": "https://api.example.com" }],
      |  "paths": {
      |    "/users": {
      |      "get": {
      |        "operationId": "listUsers",
      |        "parameters": [
      |          { "name": "page", "in": "query", "schema": { "type": "integer" }, "required": false },
      |          { "name": "email", "in": "query", "schema": { "type": "string" }, "required": true }
      |        ],
      |        "responses": {
      |          "200": {
      |            "content": {
      |              "application/json": {
      |                "schema": {
      |                  "type": "object",
      |                  "required": ["id", "name"],
      |                  "properties": {
      |                    "id": { "type": "integer", "format": "int64" },
      |                    "name": { "type": "string" },
      |                    "email": { "type": "string" },
      |                    "active": { "type": "boolean" }
      |                  }
      |                }
      |              }
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin

  test("parse simple spec extracts endpoint") {
    val result = Parser.parseContent(simpleSpec)
    assertEquals(result.baseUrl, "https://api.example.com")
    assertEquals(result.endpoints.size, 1)

    val ep = result.endpoints.head
    assertEquals(ep.path, "/users")
    assertEquals(ep.operationId, Some("listUsers"))
  }

  test("parse extracts query params") {
    val result = Parser.parseContent(simpleSpec)
    val ep = result.endpoints.head
    assertEquals(ep.queryParams.size, 2)

    val emailParam = ep.queryParams.find(_.name == "email").get
    assert(emailParam.required)
    assert(emailParam.schema.isInstanceOf[OpenAPISchema.StringType])

    val pageParam = ep.queryParams.find(_.name == "page").get
    assert(!pageParam.required)
  }

  test("parse extracts schema types") {
    val result = Parser.parseContent(simpleSpec)
    val props = result.endpoints.head.responseSchema.properties

    assert(props("id").isInstanceOf[OpenAPISchema.IntegerType])
    assertEquals(props("id").asInstanceOf[OpenAPISchema.IntegerType].format, Some("int64"))
    assert(props("name").isInstanceOf[OpenAPISchema.StringType])
    assertEquals(props("active"), OpenAPISchema.BooleanType)
  }

  test("parse extracts required fields") {
    val result = Parser.parseContent(simpleSpec)
    val required = result.endpoints.head.responseSchema.required
    assert(required.contains("id"))
    assert(required.contains("name"))
    assert(!required.contains("email"))
  }

  test("array response gets wrapped in data key") {
    val arraySpec =
      """{
        |  "openapi": "3.0.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/items": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": {
        |                  "type": "array",
        |                  "items": {
        |                    "type": "object",
        |                    "properties": {
        |                      "id": { "type": "integer" }
        |                    }
        |                  }
        |                }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val result = Parser.parseContent(arraySpec)
    assertEquals(result.endpoints.size, 1)
    val schema = result.endpoints.head.responseSchema
    assert(schema.properties.contains("data"))
    assert(schema.properties("data").isInstanceOf[OpenAPISchema.ArrayType])
  }

  test("POST-only endpoint is skipped") {
    val postSpec =
      """{
        |  "openapi": "3.0.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/create": {
        |      "post": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": { "type": "object", "properties": { "id": { "type": "integer" } } }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val result = Parser.parseContent(postSpec)
    assertEquals(result.endpoints.size, 0)
  }

  test("invalid spec throws") {
    intercept[IllegalArgumentException] {
      Parser.parseContent("not valid json at all {{{")
    }
  }
}
