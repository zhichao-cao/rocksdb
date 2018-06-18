//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/trace_analyzer_tool.h"

#include <list>
#include <utility>

#include "rocksdb/env.h"
#include "util/trace_replay.h"

namespace rocksdb {

class DBImpl;
class WriteBatch;
class AnalyzerOptions;
class TraceAnalyzer;

struct TraceUnit {
  std::string type;
  std::string key;
  uint64_t value_size;
  uint64_t ts;
};

class AnalyzerOptions {
 public:
  bool use_get;
  bool use_put;
  bool use_delete;
  bool use_merge;

  AnalyzerOptions(bool _use_get, bool _use_put, bool _use_delete,
                  bool _use_merge);

  ~AnalyzerOptions();
};

class TraceAnalyzer {
 public:
  TraceAnalyzer(std::string &trace_path, std::string &output_path,
                AnalyzerOptions _analyzer_opts);
  ~TraceAnalyzer();

  Status PrepareProcessing();

  Status StartProcessing();

  Status EndProcessing(bool need_output);

  uint64_t total_requests;
  uint64_t total_keys;
  uint64_t total_get;
  uint64_t total_write_batch;

 private:
  Env *env_;
  unique_ptr<rocksdb::TraceReader> trace_reader_;
  size_t offset_;
  char *buffer_;
  std::string trace_name_;
  std::string output_name_;
  AnalyzerOptions analyzer_opts;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
