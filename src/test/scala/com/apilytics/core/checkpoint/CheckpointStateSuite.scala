package com.apilytics.core.checkpoint

import io.circe.parser._
import munit.FunSuite

class CheckpointStateSuite extends FunSuite {

  test("Empty serializes and deserializes") {
    val state = CheckpointState.Empty
    val json = state.toJson
    val result = CheckpointState.fromJson(json)
    assertEquals(result, Right(CheckpointState.Empty))
  }

  test("CursorValue serializes and deserializes") {
    val state = CheckpointState.CursorValue("abc123")
    val json = state.toJson
    val result = CheckpointState.fromJson(json)
    assertEquals(result, Right(state))
  }

  test("TimestampValue serializes and deserializes") {
    val state = CheckpointState.TimestampValue("2024-01-15T10:30:00Z")
    val json = state.toJson
    val result = CheckpointState.fromJson(json)
    assertEquals(result, Right(state))
  }

  test("OffsetValue serializes and deserializes") {
    val state = CheckpointState.OffsetValue(42L)
    val json = state.toJson
    val result = CheckpointState.fromJson(json)
    assertEquals(result, Right(state))
  }

  test("fromJsonString round-trips") {
    val state = CheckpointState.CursorValue("cursor-xyz")
    val jsonStr = state.toJson.noSpaces
    val result = CheckpointState.fromJsonString(jsonStr)
    assertEquals(result, Right(state))
  }

  test("fromJson rejects unknown type") {
    val json = parse("""{"type": "unknown", "value": "test"}""").toOption.get
    val result = CheckpointState.fromJson(json)
    assert(result.isLeft)
    assert(result.left.toOption.get.contains("Unknown checkpoint state type"))
  }

  test("fromJsonString handles invalid JSON") {
    val result = CheckpointState.fromJsonString("not json")
    assert(result.isLeft)
  }

  test("OffsetValue handles large values") {
    val state = CheckpointState.OffsetValue(Long.MaxValue)
    val json = state.toJson
    val result = CheckpointState.fromJson(json)
    assertEquals(result, Right(state))
  }
}
