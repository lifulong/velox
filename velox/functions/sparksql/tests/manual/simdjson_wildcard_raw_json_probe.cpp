/*
 * Manual probe: mirrors simdjson/tests/ondemand/ondemand_wildcard_tests.cpp style
 * Data: only kTagMetaJson / "$[*].tag_meta" (Spark regression). Parsing style
 * follows array_wildcard_basic: result = at_path_with_wildcard(...), error(),
 * value_unsafe(), get_*().value_unsafe().
 *
 * Also checks Spark-style "$[*].tag_meta" + dom::minify for JSON text.
 *
 * Build (from velox repo root, adjust -I and lib path):
 *   c++ -std=c++17 -O2 -I/path/to/simdjson/include \
 *     ./velox/functions/sparksql/tests/manual/simdjson_wildcard_raw_json_probe.cpp \
 *     /path/to/libsimdjson.a -o simdjson_wildcard_raw_json_probe
 */

#include <simdjson.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr std::string_view kTagMetaJson =
    R"([{"tag_meta":{"meta_code":2000211,"meta_value":""},"tag_value":""}])";

constexpr std::string_view kTagMetaPath = "$[*].tag_meta";

constexpr std::string_view kExpectedTagMeta =
    R"({"meta_code":2000211,"meta_value":""})";

void printChunk(std::string_view label, std::string_view s, size_t maxLen = 500) {
  std::cerr << "--- " << label << " (len=" << s.size() << ")\n";
  if (s.size() <= maxLen) {
    std::cerr << s << "\n";
  } else {
    std::cerr << std::string_view(s.data(), maxLen) << "...(truncated)\n";
  }
}

bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

} // namespace

int main() {
  using namespace simdjson;

  // ---------- kTagMetaJson: same API as array_wildcard_basic (result / error /
  // value_unsafe / get_*().value_unsafe()). raw_json() is a separate iterate — it
  // can advance the value iterator and would break a subsequent get_object().
  padded_string padded(kTagMetaJson);
  ondemand::parser parser;

  {
    auto doc = parser.iterate(padded);
    auto result = doc.at_path_with_wildcard(kTagMetaPath);
    if (result.error()) {
      std::cerr << "tag_meta wildcard Error: " << result.error() << "\n";
      return 1;
    }
    std::vector<ondemand::value> matches = result.value_unsafe();
    if (matches.size() != 1) {
      std::cerr << "tag_meta: expected 1 match, got " << matches.size() << "\n";
      return 1;
    }
    std::string_view rawJson;
    if (!matches[0].raw_json().get(rawJson)) {
      printChunk("ondemand raw_json() [diagnostic]", rawJson);
      if (contains(rawJson, "tag_value") || rawJson != kExpectedTagMeta) {
        std::cerr << "[diagnostic] ondemand raw_json() != expected; compare dom "
                     "minify below.\n";
      }
    }
  }

  {
    auto doc = parser.iterate(padded);
    auto result = doc.at_path_with_wildcard(kTagMetaPath);
    if (result.error()) {
      std::cerr << "tag_meta wildcard (2nd iterate) Error: " << result.error()
                << "\n";
      return 1;
    }
    std::vector<ondemand::value> matches = result.value_unsafe();
    ondemand::object tagMeta = matches[0].get_object().value_unsafe();
    const int64_t metaCode = tagMeta["meta_code"].get_int64().value_unsafe();
    const std::string_view metaValue =
        tagMeta["meta_value"].get_string().value_unsafe();
    if (metaCode != 2000211 || metaValue != "") {
      std::cerr << "tag_meta: field mismatch meta_code=" << metaCode << "\n";
      return 1;
    }
    std::cerr << "[ondemand typed] tag_meta meta_code=" << metaCode
              << " meta_value_len=" << metaValue.size() << " OK\n";
  }

  // ---------- DOM + minify for exact JSON text (Spark get_json_object string)
  dom::parser domParser;
  dom::element root;
  if (domParser.parse(padded).get(root)) {
    std::cerr << "dom::parser::parse failed\n";
    return 1;
  }

  auto domResult = root.at_path_with_wildcard(kTagMetaPath);
  if (domResult.error()) {
    std::cerr << "dom at_path_with_wildcard Error: " << domResult.error() << "\n";
    return 1;
  }
  std::vector<dom::element> domMatches = domResult.value_unsafe();
  if (domMatches.size() != 1) {
    std::cerr << "dom: expected 1 match\n";
    return 1;
  }

  const std::string minified = minify(domMatches[0]);
  printChunk("dom minify(match[0])", minified);
  std::cerr << "\nExpected tag_meta JSON:\n" << kExpectedTagMeta << "\n";

  if (minified != std::string(kExpectedTagMeta)) {
    std::cerr << "\nFAIL: dom minify != expected.\n";
    return 2;
  }

  std::cerr << "\nOK: kTagMetaJson + array_wildcard_basic-style parse + dom minify.\n";
  return 0;
}
