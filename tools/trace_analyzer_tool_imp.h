//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/trace_analyzer_tool.h"

#include <stdio.h>
#include <fstream>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <utility>

#include "rocksdb/env.h"
#include "util/trace_replay.h"

namespace rocksdb {

class DBImpl;
class WriteBatch;
class AnalyzerOptions;
class TraceAnalyzer;

struct TraceUnit {
  uint32_t type;
  std::string key;
  size_t value_size;
  uint64_t ts;
  uint32_t cf_id;
};

struct StatsUnit {
  uint64_t key_id;
  uint32_t cf_id;
  size_t value_size;
  uint64_t access_count;
};


class AnalyzerOptions {
 public:
  bool output_key_stats;
  bool output_access_count_stats;
  bool output_trace_unit;
  bool output_time_serial;
  bool output_prefix_cut;
  bool input_key_space;
  bool use_get;
  bool use_put;
  bool use_delete;
  bool use_merge;
  bool no_key;
  bool print_overall_stats;
  bool print_key_distribution;
  bool print_value_distribution;
  bool print_top_k_access;
  uint64_t  output_ignore_count;
  uint64_t start_time;
  int  value_interval;
  int top_k;
  int prefix_cut;
  std::string output_prefix;
  std::string key_space_dir;

  AnalyzerOptions();

  ~AnalyzerOptions();
};


struct TraceStats {
  uint32_t cf_id;
  std::string cf_name;
  uint64_t get_count;
  uint64_t total_count;
  uint64_t whole_key_space_count;
  std::map<std::string, StatsUnit> key_stats;
  std::map<uint64_t, uint64_t> access_count_stats;
  std::map<uint64_t, uint64_t> key_size_stats;
  std::map<uint64_t, uint64_t> value_size_stats;
  std::priority_queue<std::pair<uint64_t, std::string>,
                      std::vector<std::pair<uint64_t, std::string>>,
                      std::greater<std::pair<uint64_t, std::string>>> top_k_queue;
  std::list<TraceUnit> time_serial;
  FILE *trace_unit_file;
};


class TraceAnalyzer {
 public:
  TraceAnalyzer(std::string &trace_path, std::string &output_path,
                AnalyzerOptions _analyzer_opts);
  ~TraceAnalyzer();

  Status PrepareProcessing();

  Status StartProcessing();

  Status MakeStatistics();

  Status ReProcessing();

  Status EndProcessing();

  Status WriteTraceUnit(TraceUnit &unit);

  /*
  // The write batch processing functions
  Status DealPutCF(uint32_t column_family_id, const Slice& key, const Slice& value);
  Status DealDeleteCF(uint32_t column_family_id, const Slice& key);
  Status DealSingleDeleteCF(uint32_t column_family_id, const Slice& key);
  Status DealDeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
        const Slice& end_key);
  Status DealMergeCF(uint32_t column_family_id, const Slice& key, const Slice& value);
  */

  uint64_t total_requests;
  uint64_t total_keys;
  uint64_t total_get;
  uint64_t total_write_batch;

 private:
  Env *env_;
  unique_ptr<rocksdb::TraceReader> trace_reader_;
  size_t offset_;
  char *buffer_;
  uint64_t guid_;
  std::string trace_name_;
  std::string output_path_;
  bool need_output_;
  AnalyzerOptions analyzer_opts_;
  std::map<std::string, TraceStats> get_map_;
  std::map<uint32_t, TraceStats> write_map_;

  Status TraceStatsInsertionGet(TraceUnit &unit, TraceStats& stats);
  Status TraceStatsInsertionWrite(TraceUnit &unit, TraceStats& stats);

  void PrintGetStatistics();
  Status TraceUnitWriter(FILE *file_p, TraceUnit &unit);
  std::string MicrosdToDate(uint64_t time);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
