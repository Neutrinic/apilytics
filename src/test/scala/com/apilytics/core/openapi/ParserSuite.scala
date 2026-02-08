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

  test("additionalProperties: true produces VariantType") {
    val spec =
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
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "metadata": { "type": "object", "additionalProperties": true }
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    assertEquals(props("metadata"), OpenAPISchema.VariantType)
  }

  test("empty object (no properties) produces VariantType") {
    val spec =
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
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "extra": { "type": "object" }
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    assertEquals(props("extra"), OpenAPISchema.VariantType)
  }

  test("missing type produces VariantType") {
    val spec =
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
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "blob": {}
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    assertEquals(props("blob"), OpenAPISchema.VariantType)
  }

  test("oneOf produces VariantType") {
    val spec =
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
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "value": {
        |                      "oneOf": [
        |                        { "type": "string" },
        |                        { "type": "integer" }
        |                      ]
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    assertEquals(props("value"), OpenAPISchema.VariantType)
  }

  test("anyOf produces VariantType") {
    val spec =
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
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "payload": {
        |                      "anyOf": [
        |                        { "type": "string" },
        |                        { "type": "object", "properties": { "key": { "type": "string" } } }
        |                      ]
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    assertEquals(props("payload"), OpenAPISchema.VariantType)
  }

  // ==========================================================================
  // OpenAPI 3.1 compatibility tests
  // ==========================================================================

  test("OpenAPI 3.1: type array with null (nullable) is parsed") {
    // In OpenAPI 3.1, nullable is expressed as type: ["string", "null"]
    val spec =
      """{
        |  "openapi": "3.1.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/items": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": {
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "nickname": { "type": ["string", "null"] }
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    // swagger-parser converts type array to the first non-null type
    // If it doesn't, we'd get UnknownType or VariantType
    assert(
      props("nickname").isInstanceOf[OpenAPISchema.StringType] ||
      props("nickname") == OpenAPISchema.VariantType,
      s"Expected StringType or VariantType for nullable string, got ${props("nickname")}"
    )
  }

  test("OpenAPI 3.1: basic spec parses correctly") {
    val spec =
      """{
        |  "openapi": "3.1.0",
        |  "info": { "title": "Test 3.1", "version": "1.0" },
        |  "paths": {
        |    "/users": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": {
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "name": { "type": "string" }
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

    val result = Parser.parseContent(spec)
    assertEquals(result.endpoints.size, 1)
    assertEquals(result.endpoints.head.path, "/users")
    val props = result.endpoints.head.responseSchema.properties
    assert(props("id").isInstanceOf[OpenAPISchema.IntegerType])
    assert(props("name").isInstanceOf[OpenAPISchema.StringType])
  }

  test("OpenAPI 3.1: exclusiveMinimum as number (not boolean) parses") {
    // In 3.0: exclusiveMinimum: true with minimum: 0
    // In 3.1: exclusiveMinimum: 0 (just the number)
    val spec =
      """{
        |  "openapi": "3.1.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/items": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": {
        |                  "type": "object",
        |                  "properties": {
        |                    "count": {
        |                      "type": "integer",
        |                      "exclusiveMinimum": 0
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    // Should still parse as integer, exclusiveMinimum is just validation metadata
    assert(props("count").isInstanceOf[OpenAPISchema.IntegerType])
  }

  test("OpenAPI 3.1: const value parses") {
    // OpenAPI 3.1 supports JSON Schema const
    val spec =
      """{
        |  "openapi": "3.1.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/items": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": {
        |                  "type": "object",
        |                  "properties": {
        |                    "version": { "type": "string", "const": "v1" },
        |                    "id": { "type": "integer" }
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    // const is validation metadata, type should still be string
    assert(props("version").isInstanceOf[OpenAPISchema.StringType])
    assert(props("id").isInstanceOf[OpenAPISchema.IntegerType])
  }

  test("OpenAPI 3.1: $defs at root level is NOT supported by swagger-parser".ignore) {
    // LIMITATION: swagger-parser 2.1.x does not support $defs at root level.
    // It reports: "attribute $defs is unexpected"
    // Workaround: Use components/schemas instead of root-level $defs
    //
    // Note: OpenAPI 3.1 allows $defs at root level per JSON Schema 2020-12,
    // but swagger-parser doesn't handle this yet.
    val spec =
      """{
        |  "openapi": "3.1.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "$defs": {
        |    "User": {
        |      "type": "object",
        |      "properties": {
        |        "id": { "type": "integer" },
        |        "name": { "type": "string" }
        |      }
        |    }
        |  },
        |  "paths": {
        |    "/users": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": { "$ref": "#/$defs/User" }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val result = Parser.parseContent(spec)
    assertEquals(result.endpoints.size, 1)
  }

  test("OpenAPI 3.1: union type array produces VariantType") {
    // In OpenAPI 3.1, type: ["string", "integer"] is a true union (not nullable)
    // This should produce VariantType, not silently pick the first type
    val spec =
      """{
        |  "openapi": "3.1.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/items": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "content": {
        |              "application/json": {
        |                "schema": {
        |                  "type": "object",
        |                  "properties": {
        |                    "id": { "type": "integer" },
        |                    "value": { "type": ["string", "integer"] }
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

    val result = Parser.parseContent(spec)
    val props = result.endpoints.head.responseSchema.properties
    // Union of string|integer should be VariantType, not StringType
    assertEquals(props("value"), OpenAPISchema.VariantType)
  }

  test("OpenAPI 3.1: components/schemas references work") {
    // Use components/schemas (the standard OpenAPI way) instead of $defs
    val spec =
      """{
        |  "openapi": "3.1.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "components": {
        |    "schemas": {
        |      "User": {
        |        "type": "object",
        |        "properties": {
        |          "id": { "type": "integer" },
        |          "name": { "type": "string" }
        |        }
        |      }
        |    }
        |  },
        |  "paths": {
        |    "/users": {
        |      "get": {
        |        "responses": {
        |          "200": {
        |            "description": "OK",
        |            "content": {
        |              "application/json": {
        |                "schema": { "$ref": "#/components/schemas/User" }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val result = Parser.parseContent(spec)
    assertEquals(result.endpoints.size, 1)
    val props = result.endpoints.head.responseSchema.properties
    assert(props("id").isInstanceOf[OpenAPISchema.IntegerType])
    assert(props("name").isInstanceOf[OpenAPISchema.StringType])
  }
}
