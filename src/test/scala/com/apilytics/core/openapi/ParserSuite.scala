package com.apilytics.core.openapi

import com.apilytics.core.schema.SourceSchema

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
    assert(emailParam.schema.isInstanceOf[SourceSchema.StringType])

    val pageParam = ep.queryParams.find(_.name == "page").get
    assert(!pageParam.required)
  }

  test("parse extracts schema types") {
    val result = Parser.parseContent(simpleSpec)
    val props = result.endpoints.head.responseSchema.properties

    assert(props("id").isInstanceOf[SourceSchema.IntegerType])
    assertEquals(props("id").asInstanceOf[SourceSchema.IntegerType].format, Some("int64"))
    assert(props("name").isInstanceOf[SourceSchema.StringType])
    assertEquals(props("active"), SourceSchema.BooleanType)
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
    assert(schema.properties("data").isInstanceOf[SourceSchema.ArrayType])
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
    assertEquals(props("metadata"), SourceSchema.VariantType)
  }

  /** An object with `id` and `name` declared, and `additionalProperties` set to `ap`. */
  private def objectWithAdditionalProperties(ap: String): SourceSchema = {
    val spec =
      s"""{ "openapi": "3.0.0", "info": { "title": "t", "version": "1" }, "paths": { "/items": { "get": {
         |  "responses": { "200": { "description": "ok", "content": { "application/json": { "schema": {
         |    "type": "object",
         |    "properties": { "row": {
         |      "type": "object",
         |      "properties": { "id": { "type": "integer" }, "name": { "type": "string" } },
         |      "additionalProperties": $ap
         |    } }
         |  } } } } } } } } }""".stripMargin
    Parser.parseContent(spec).endpoints.head.responseSchema.properties("row")
  }

  test("declared properties stay typed columns whatever additionalProperties says (#341)") {
    // `true` only allows fields beyond those listed, which is the default anyway. It used to
    // turn the object into a VARIANT and throw its declared fields away.
    val expected = SourceSchema.ObjectType(Map(
      "id"   -> SourceSchema.IntegerType(),
      "name" -> SourceSchema.StringType()
    ))
    for (ap <- List("true", "false", """{ "type": "string" }"""))
      assertEquals(objectWithAdditionalProperties(ap), expected, s"additionalProperties: $ap")
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
    assertEquals(props("extra"), SourceSchema.VariantType)
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
    assertEquals(props("blob"), SourceSchema.VariantType)
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
    assertEquals(props("value"), SourceSchema.VariantType)
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
    assertEquals(props("payload"), SourceSchema.VariantType)
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
      props("nickname").isInstanceOf[SourceSchema.StringType] ||
      props("nickname") == SourceSchema.VariantType,
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
    assert(props("id").isInstanceOf[SourceSchema.IntegerType])
    assert(props("name").isInstanceOf[SourceSchema.StringType])
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
    assert(props("count").isInstanceOf[SourceSchema.IntegerType])
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
    assert(props("version").isInstanceOf[SourceSchema.StringType])
    assert(props("id").isInstanceOf[SourceSchema.IntegerType])
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
    assertEquals(props("value"), SourceSchema.VariantType)
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
    assert(props("id").isInstanceOf[SourceSchema.IntegerType])
    assert(props("name").isInstanceOf[SourceSchema.StringType])
  }

  // ==========================================================================
  // Swagger 2.0 compatibility tests (#138)
  // ==========================================================================

  test("Swagger 2.0: basic spec parses correctly") {
    // Swagger 2.0 uses "swagger": "2.0" instead of "openapi"
    // and has different response structure (no "content" wrapper)
    val spec =
      """{
        |  "swagger": "2.0",
        |  "info": { "title": "Test Swagger 2.0", "version": "1.0" },
        |  "host": "api.example.com",
        |  "basePath": "/v1",
        |  "schemes": ["https"],
        |  "paths": {
        |    "/users": {
        |      "get": {
        |        "produces": ["application/json"],
        |        "responses": {
        |          "200": {
        |            "description": "OK",
        |            "schema": {
        |              "type": "object",
        |              "properties": {
        |                "id": { "type": "integer" },
        |                "name": { "type": "string" }
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
    assert(props("id").isInstanceOf[SourceSchema.IntegerType])
    assert(props("name").isInstanceOf[SourceSchema.StringType])
  }

  test("Swagger 2.0: array response parses") {
    val spec =
      """{
        |  "swagger": "2.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/items": {
        |      "get": {
        |        "produces": ["application/json"],
        |        "responses": {
        |          "200": {
        |            "description": "OK",
        |            "schema": {
        |              "type": "array",
        |              "items": {
        |                "type": "object",
        |                "properties": {
        |                  "id": { "type": "integer" },
        |                  "name": { "type": "string" }
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
    // Array responses get wrapped in "data" key
    assert(result.endpoints.head.responseSchema.properties.contains("data"))
  }

  test("Swagger 2.0: query parameters parse") {
    val spec =
      """{
        |  "swagger": "2.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "paths": {
        |    "/users": {
        |      "get": {
        |        "produces": ["application/json"],
        |        "parameters": [
        |          { "name": "page", "in": "query", "type": "integer", "required": false },
        |          { "name": "limit", "in": "query", "type": "integer", "required": true }
        |        ],
        |        "responses": {
        |          "200": {
        |            "description": "OK",
        |            "schema": {
        |              "type": "object",
        |              "properties": {
        |                "id": { "type": "integer" }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val result = Parser.parseContent(spec)
    val ep = result.endpoints.head
    assertEquals(ep.queryParams.size, 2)

    val pageParam = ep.queryParams.find(_.name == "page").get
    assert(!pageParam.required)

    val limitParam = ep.queryParams.find(_.name == "limit").get
    assert(limitParam.required)
  }

  test("Swagger 2.0: definitions references work") {
    // Swagger 2.0 uses "definitions" instead of "components/schemas"
    val spec =
      """{
        |  "swagger": "2.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "definitions": {
        |    "User": {
        |      "type": "object",
        |      "properties": {
        |        "id": { "type": "integer" },
        |        "email": { "type": "string" }
        |      }
        |    }
        |  },
        |  "paths": {
        |    "/users": {
        |      "get": {
        |        "produces": ["application/json"],
        |        "responses": {
        |          "200": {
        |            "description": "OK",
        |            "schema": { "$ref": "#/definitions/User" }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val result = Parser.parseContent(spec)
    assertEquals(result.endpoints.size, 1)
    val props = result.endpoints.head.responseSchema.properties
    assert(props("id").isInstanceOf[SourceSchema.IntegerType])
    assert(props("email").isInstanceOf[SourceSchema.StringType])
  }

  test("Swagger 2.0: baseUrl is constructed from host/basePath/schemes") {
    val spec =
      """{
        |  "swagger": "2.0",
        |  "info": { "title": "Test", "version": "1.0" },
        |  "host": "api.example.com",
        |  "basePath": "/v2",
        |  "schemes": ["https"],
        |  "paths": {
        |    "/items": {
        |      "get": {
        |        "produces": ["application/json"],
        |        "responses": {
        |          "200": {
        |            "description": "OK",
        |            "schema": {
        |              "type": "object",
        |              "properties": { "id": { "type": "integer" } }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin

    val result = Parser.parseContent(spec)
    // SwaggerConverter constructs servers[0].url from host+basePath+schemes
    assertEquals(result.baseUrl, "https://api.example.com/v2")
  }

  test("Swagger 2.0: Petstore spec from URL") {
    // Served locally, so the test needs no internet access (#320). It used to fetch
    // petstore.swagger.io, and failed offline or behind a proxy.
    val petstore =
      """{
        |  "swagger": "2.0",
        |  "info": { "title": "Swagger Petstore", "version": "1.0.7" },
        |  "host": "petstore.swagger.io",
        |  "basePath": "/v2",
        |  "schemes": ["https"],
        |  "paths": {
        |    "/pet/findByStatus": {
        |      "get": {
        |        "operationId": "findPetsByStatus",
        |        "produces": ["application/json"],
        |        "parameters": [{ "name": "status", "in": "query", "required": true, "type": "array", "items": { "type": "string" } }],
        |        "responses": { "200": { "description": "ok", "schema": { "type": "array", "items": { "$ref": "#/definitions/Pet" } } } }
        |      }
        |    },
        |    "/store/inventory": {
        |      "get": {
        |        "operationId": "getInventory",
        |        "produces": ["application/json"],
        |        "responses": { "200": { "description": "ok", "schema": { "type": "object", "additionalProperties": { "type": "integer" } } } }
        |      }
        |    }
        |  },
        |  "definitions": {
        |    "Pet": { "type": "object", "properties": { "id": { "type": "integer", "format": "int64" }, "name": { "type": "string" } } }
        |  }
        |}""".stripMargin
    val server = new com.github.tomakehurst.wiremock.WireMockServer(
      com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig().dynamicPort()
    )
    server.start()
    try {
      import com.github.tomakehurst.wiremock.client.WireMock._
      server.stubFor(get(urlPathEqualTo("/v2/swagger.json")).willReturn(okJson(petstore)))

      val result = Parser.parse(s"http://localhost:${server.port()}/v2/swagger.json")

      val paths = result.endpoints.map(_.path).toSet
      assert(paths.contains("/pet/findByStatus"), s"Expected /pet/findByStatus, got: $paths")
      // The base URL comes from the spec's host and basePath, not from where it was fetched.
      assertEquals(result.baseUrl, "https://petstore.swagger.io/v2")
      // Array response gets wrapped in "data" key
      val findByStatus = result.endpoints.find(_.path == "/pet/findByStatus").get
      assert(findByStatus.responseSchema.properties.contains("data"))
    } finally server.stop()
  }

  /** A spec with one GET per content type, each returning `{id}`. */
  private def specWithContent(entries: (String, String)*): String = {
    val paths = entries.map { case (path, types) =>
      val content = types.split('|').map(ct =>
        s""""$ct": { "schema": { "type": "object", "properties": { "${ct.filter(_.isLetter).take(8)}": { "type": "string" } } } }"""
      ).mkString(", ")
      s""""$path": { "get": { "responses": { "200": { "description": "ok", "content": { $content } } } } }"""
    }.mkString(", ")
    s"""{ "openapi": "3.0.0", "info": { "title": "t", "version": "1" }, "paths": { $paths } }"""
  }

  test("JSON responses are read under any JSON media type (#319)") {
    val result = Parser.parseContent(specWithContent(
      "/charset"  -> "application/json; charset=utf-8",
      "/hal"      -> "application/hal+json",
      "/jsonapi"  -> "application/vnd.api+json",
      "/wildcard" -> "*/*",
      "/text"     -> "text/plain",
      "/xml"      -> "application/xml|text/csv"
    ))

    assertEquals(result.endpoints.map(_.path).toSet, Set("/charset", "/hal", "/jsonapi", "/wildcard"))
    assertEquals(result.unreadable, Map("/text" -> List("text/plain"), "/xml" -> List("application/xml", "text/csv")))
  }

  test("an exact application/json is preferred to the wildcard (#319)") {
    // The schema's one property is named after the media type it came from.
    val result = Parser.parseContent(specWithContent("/both" -> "*/*|application/json"))
    assertEquals(result.endpoints.head.responseSchema.properties.keySet, Set("applicat"))
  }

  test("a +json type outside application/ is read, and a schema-less type doesn't win (#319)") {
    val spec =
      """{ "openapi": "3.0.0", "info": { "title": "t", "version": "1" }, "paths": {
        |  "/vendor": { "get": { "responses": { "200": { "description": "ok", "content": {
        |    "text/vnd.example+json": { "schema": { "type": "object", "properties": { "v": { "type": "string" } } } } } } } } },
        |  "/split": { "get": { "responses": { "200": { "description": "ok", "content": {
        |    "application/json": {},
        |    "application/hal+json": { "schema": { "type": "object", "properties": { "h": { "type": "string" } } } } } } } } }
        |} }""".stripMargin
    val endpoints = Parser.parseContent(spec).endpoints.map(e => e.path -> e.responseSchema.properties.keySet).toMap
    assertEquals(endpoints, Map("/vendor" -> Set("v"), "/split" -> Set("h")))
  }

  /** A spec whose /export answers with `contentType` and `schema` (JSON). */
  private def ndjsonSpec(contentType: String, schema: String): ParsedSpec =
    Parser.parseContent(
      s"""{ "openapi": "3.0.0", "info": { "title": "t", "version": "1" }, "paths": { "/export": { "get": {
         |  "responses": { "200": { "description": "ok", "content": { "$contentType": { "schema": $schema } } } } } } } }""".stripMargin
    )

  private val record = """{ "type": "object", "properties": { "id": { "type": "integer" }, "name": { "type": "string" } } }"""
  private val recordSchema = SourceSchema.ObjectType(Map("id" -> SourceSchema.IntegerType(), "name" -> SourceSchema.StringType()))

  test("an NDJSON response's record schema is kept for NDJSON sources (#345)") {
    // Stored as a top-level array response is, `data` wrapping the record, which table
    // resolution unwraps.
    val wrapped = SourceSchema.ObjectType(Map("data" -> SourceSchema.ArrayType(recordSchema)))
    for (ct <- List("application/x-ndjson", "application/jsonl", "application/x-ndjson; charset=utf-8")) {
      val spec = ndjsonSpec(ct, record)
      assertEquals(spec.ndjsonEndpoints.map(e => e.path -> e.responseSchema), List("/export" -> wrapped), ct)
      assertEquals(spec.endpoints, Nil, s"$ct is not a JSON response")
    }
    // Some specs describe the stream as a whole: an array whose items are the records.
    assertEquals(ndjsonSpec("application/x-ndjson", s"""{ "type": "array", "items": $record }""").ndjsonEndpoints.map(_.responseSchema), List(wrapped))
  }

  test("an NDJSON response without a record schema gives no columns (#345)") {
    assertEquals(ndjsonSpec("application/x-ndjson", """{ "type": "string" }""").ndjsonEndpoints, Nil)
    // json-seq starts each record with an RS character, which the NDJSON reader can't parse.
    assertEquals(ndjsonSpec("application/json-seq", record).ndjsonEndpoints, Nil)
  }
}
