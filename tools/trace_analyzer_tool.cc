//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/trace_analyzer_tool_imp.h"

#include <inttypes.h>
#include <math.h>
#include <sys/stat.h>
#include <time.h>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "rocksdb/write_batch.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/file_reader_writer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/gflags_compat.h"
#include "util/trace_replay.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_string(trace_file, "", "The trace path.");
DEFINE_string(output_dir. "", "The directory to store the output files.");
DEFINE_string(output_prefix, "", "The prefix used for all the output files.");
DEFINE_bool(output_key_stats, false, " Output the key access count statistics to file"
            "for accessed keys:"
            "format:[cf_id value_size access_keyid access_count]"
            "for the whole key space keys:"
            "format:[whole_key_space_keyid access_count]");
DEFINE_bool(output_access_count_stats, false,
            "Output the access count distribution statistics to file."
            "format:[access_count number_of_access_count]");
DEFINE_uint64(output_time_series, 0,
              "trace collect time, in microseconds"
              "Output the access time sequence of each key"
              "format:[type_id time_in_sec access_key_id]");
DEFINE_int32(output_prefix_cut, 0,
            "The number of bytes as prefix to cut the keys."
            "if it is enabled, it will generate the following:"
            "for accessed keys:"
            "format:[acessed_keyid access_count num_keys ave_access prefix]"
            "for whole key space keys:"
            "format:[start_keyid_in_whole_keyspace prefix]"
            "if 'output_qps_stats' is enabled, it will output:"
            "format:[time_in_sec IO_num], [prefix qps_of_this_second]");
DEFINE_bool(output_trace_sequence, false,
            "Out put the trace sequence for further processing"
            "including the type, cf_id, ts, value_sze, key. This file"
            "will be extremely large (similar size as the original trace)"
            "you can specify 'no_key' to reduce the size"
            "format:[type_id cf_id value_size time_in_micorsec <key>]");
DEFINE_bool(output_qps_stats, false,
            "Output the query per second(qps) statistics"
            "For the overall qps, it will contain all qps of each query type."
            "For each cf and query, it will have its own qps output"
            "format:[query_count_in_this_second]");
DEFINE_string(output_correlation, "",
            "intput format: [correlation pairs][.,.]"
            "Output the query correlations between the pairs of query types"
            "listed in the parameter, input should select the operations from:"
            "get, put, delete, single_delete, rangle_delete, merge. No space"
            "between the pairs separated by commar. Example: =[get,get][put,get]");
DEFINE_bool(input_key_space, false, );
DEFINE_bool(use_get, false, );
DEFINE_bool(use_put, false, );
DEFINE_bool(use_delete, false, );
DEFINE_bool(use_single_delete, false, );
DEFINE_bool(use_range_delete, false, );
DEFINE_bool(use_merge, false, );
DEFINE_bool(use_iterator, false, );
DEFINE_bool(no_key, true, );
DEFINE_bool(print_overall_stats, false, );
DEFINE_bool(print_key_distribution, false, );
DEFINE_bool(print_value_distribution, false, );
DEFINE_bool(print_top_k_access, false, );
DEFINE_bool(special, false, );
DEFINE_bool(output_ignore_count, false, );
DDEFINE_bool(value_interval, false, );
DEFINE_bool(top_k, false, );
DEFINE_bool(prefix_cut, false, );
EFINE_bool(, false, );
DEFINE_bool(, false, );
DEFINE_bool(, false, );










namespace rocksdb {

std::map<std::string, int> taOptToIndex = {
    {"get", 0},          {"put", 1},   {"delete", 2},  {"single_delete", 3},
    {"range_delete", 4}, {"merge", 5}, {"iterator", 6}};

std::map<int, std::string> taIndexToOpt = {
    {0, "get"},          {1, "put"},   {2, "delete"},  {3, "single_delete"},
    {4, "range_delete"}, {5, "merge"}, {6, "iterator"}};

// Transfer the Microsecond time to date time
std::string TraceAnalyzer::MicrosdToDate(uint64_t time_in) {
  time_t tx = static_cast<time_t>(time_in / 1000000);
  int rest = static_cast<int>(time_in % 1000000);
  std::string date_time(ctime(&tx));
  date_time.pop_back();
  date_time += " +: " + std::to_string(rest);
  return date_time;
}

namespace {
bool ReadOneLine(std::istringstream* iss, SequentialFile* seq_file,
                 std::string* output, bool* has_data, Status* result) {
  const int kBufferSize = 128;
  char buffer[kBufferSize + 1];
  Slice input_slice;

  std::string line;
  bool has_complete_line = false;
  while (!has_complete_line) {
    if (std::getline(*iss, line)) {
      has_complete_line = !iss->eof();
    } else {
      has_complete_line = false;
    }
    if (!has_complete_line) {
      // if we're not sure whether we have a complete line,
      // further read from the file.
      if (*has_data) {
        *result = seq_file->Read(kBufferSize, &input_slice, buffer);
      }
      if (input_slice.size() == 0) {
        // meaning we have read all the data
        *has_data = false;
        break;
      } else {
        iss->str(line + input_slice.ToString());
        // reset the internal state of iss so that we can keep reading it.
        iss->clear();
        *has_data = (input_slice.size() == kBufferSize);
        continue;
      }
    }
  }
  *output = line;
  return *has_data || has_complete_line;
}
}  // namespace


// The default constructor of AnalyzerOptions
AnalyzerOptions::AnalyzerOptions()
    : corre_map(taTypeNum, std::vector<int>(taTypeNum, -1)) {
  output_key_stats = false;
  output_access_count_stats = false;
  output_time_serial = false;
  output_prefix_cut = false;
  output_trace_sequence = false;
  output_io_stats = false;
  output_correlation = false;
  input_key_space = false;
  use_get = false;
  use_put = false;
  use_delete = false;
  use_single_delete = false;
  use_range_delete = false;
  use_merge = false;
  use_iterator = false;
  no_key = false;
  print_overall_stats = false;
  print_key_distribution = false;
  print_value_distribution = false;
  print_top_k_access = true;
  special = false;
  output_ignore_count = 0;
  start_time = 0;
  value_interval = 8;
  top_k = 1;
  prefix_cut = 0;
  output_prefix = "/trace_output";
  key_space_dir = "./";
}

AnalyzerOptions::~AnalyzerOptions() {}

void AnalyzerOptions::SparseCorreInput(const std::string& in_str) {
  std::string cur = in_str;
  if (cur.size() == 0) {
    output_correlation = false;
    return;
  }
  while (!cur.empty()) {
    if (cur.compare(0, 1, "[") != 0) {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    std::string opt1, opt2;
    std::size_t split = cur.find_first_of(",");
    if (split != std::string::npos) {
      opt1 = cur.substr(1, split - 1);
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    std::size_t end = cur.find_first_of("]");
    if (end != std::string::npos) {
      opt2 = cur.substr(split + 1, end - split - 1);
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
    cur = cur.substr(end + 1);

    if (taOptToIndex.find(opt1) != taOptToIndex.end() &&
        taOptToIndex.find(opt2) != taOptToIndex.end()) {
      corre_list.push_back(
          std::make_pair(taOptToIndex[opt1], taOptToIndex[opt2]));
    } else {
      fprintf(stderr, "Invalid correlation input: %s\n", in_str.c_str());
      exit(1);
    }
  }

  int sequence = 0;
  for (auto it = corre_list.begin(); it != corre_list.end(); it++) {
    corre_map[it->first][it->second] = sequence;
    sequence++;
  }
  return;
}

// The trace statistic struct constructor
TraceStats::TraceStats() {
  cf_id = 0;
  cf_name = "0";
  a_count = 0;
  akey_id = 0;
  a_key_size_sqsum = 0;
  a_key_size_sum = 0;
  a_key_mid = 0;
  a_value_size_sqsum = 0;
  a_value_size_sum = 0;
  a_value_mid = 0;
  a_peak_io = 0;
  a_ave_io = 0.0;
}

TraceStats::~TraceStats() {}

// The trace analyzer constructor
TraceAnalyzer::TraceAnalyzer(std::string& trace_path, std::string& output_path,
                             AnalyzerOptions _analyzer_opts)
    : trace_name_(trace_path),
      output_path_(output_path),
      analyzer_opts_(_analyzer_opts) {
  rocksdb::EnvOptions env_options;
  env_ = rocksdb::Env::Default();
  offset_ = 0;
  buffer_ = new char[1024];
  c_time_ = 0;
  total_requests_ = 0;
  total_access_keys_ = 0;
  total_gets_ = 0;
  total_writes_ = 0;
  begin_time_ = 0;
  end_time_ = 0;
  ta_.resize(taTypeNum);
  ta_[0].type_name = "get";
  if (_analyzer_opts.use_get) {
    ta_[0].enabled = true;
  } else {
    ta_[0].enabled = false;
  }
  ta_[1].type_name = "put";
  if (_analyzer_opts.use_put) {
    ta_[1].enabled = true;
  } else {
    ta_[1].enabled = false;
  }
  ta_[2].type_name = "delete";
  if (_analyzer_opts.use_delete) {
    ta_[2].enabled = true;
  } else {
    ta_[2].enabled = false;
  }
  ta_[3].type_name = "single_delete";
  if (_analyzer_opts.use_single_delete) {
    ta_[3].enabled = true;
  } else {
    ta_[3].enabled = false;
  }
  ta_[4].type_name = "range_delete";
  if (_analyzer_opts.use_range_delete) {
    ta_[4].enabled = true;
  } else {
    ta_[4].enabled = false;
  }
  ta_[5].type_name = "merge";
  if (_analyzer_opts.use_merge) {
    ta_[5].enabled = true;
  } else {
    ta_[5].enabled = false;
  }
  ta_[6].type_name = "iterator";
  if (_analyzer_opts.use_iterator) {
    ta_[6].enabled = true;
  } else {
    ta_[6].enabled = false;
  }
}

TraceAnalyzer::~TraceAnalyzer() { delete buffer_; }

// Prepare the processing
// Initiate the global trace reader and writer here
Status TraceAnalyzer::PrepareProcessing() {
  Status s;
  // Prepare the trace reader
  unique_ptr<rocksdb::RandomAccessFile> trace_file;
  s = env_->NewRandomAccessFile(trace_name_, &trace_file, env_options_);
  if (!s.ok()) {
    return s;
  }
  unique_ptr<rocksdb::RandomAccessFileReader> trace_file_reader;
  trace_file_reader.reset(
      new rocksdb::RandomAccessFileReader(std::move(trace_file), trace_name_));
  trace_reader_.reset(new rocksdb::TraceReader(std::move(trace_file_reader)));

  // Prepare and open the trace sequence file writer if needed
  if (analyzer_opts_.output_trace_sequence) {
    std::string trace_sequence_name;
    trace_sequence_name = output_path_ + "/" + analyzer_opts_.output_prefix +
                          "-trace_sequence.txt";
    s = env_->NewWritableFile(trace_sequence_name, &trace_sequence_f_,
                              env_options_);
    if (!s.ok()) {
      return s;
    }
  }

  // prepare the general QPS file writer
  if (analyzer_opts_.output_io_stats) {
    std::string qps_stats_name;
    qps_stats_name =
        output_path_ + "/" + analyzer_opts_.output_prefix + "-qps_stats.txt";
    s = env_->NewWritableFile(qps_stats_name, &qps_f_, env_options_);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

// process the trace itself and redirect the trace content
// to different operation type handler. With different race
// format, this function can be changed
Status TraceAnalyzer::StartProcessing() {
  Status s;
  Trace header;
  s = trace_reader_->ReadHeader(header);
  if (!s.ok()) {
    return s;
  }

  Trace footer;
  s = trace_reader_->ReadFooter(footer);
  if (!s.ok()) {
    return s;
  }

  Trace trace;
  while (s.ok()) {
    trace.reset();
    s = trace_reader_->ReadRecord(trace);
    if (!s.ok()) {
      break;
    }

    total_requests_++;
    end_time_ = trace.ts;
    if (trace.type == kTraceWrite) {
      total_writes_++;
      c_time_ = trace.ts;
      WriteBatch batch(trace.payload);
      if (batch.HasBeginPrepare() && !batch.HasCommit()) {
        continue;
      }
      TraceWriteHandler write_handler(this);
      s = batch.Iterate(&write_handler);
      if (!s.ok()) {
        fprintf(stderr, "Cannot process the write batch in the trace\n");
        exit(1);
      }
    } else if (trace.type == kTraceGet) {
      total_gets_++;
      s = HandleGetCF(trace.cf_id, trace.payload, trace.ts, trace.get_ret);
      if (!s.ok()) {
        fprintf(stderr, "Cannot process the get in the trace\n");
        exit(1);
      }
    } else if (trace.type == kTraceIter) {
      s = HandleIterCF(trace.cf_id, trace.payload, trace.ts);
      if (!s.ok()) {
        fprintf(stderr, "Cannot process the iterator in the trace\n");
        exit(1);
      }
    }
  }
  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
    //
    return Status::OK();
  }
  return s;
}

Status TraceAnalyzer::SpecialProcessing() {
  Status s;
  Trace header;
  begin_time_ = 0;
  std::string name;
  std::map<uint32_t, FILE*> f_map;
  uint32_t time_in_sec;
  /*
  name = "67062.txt";
  f_map[67062] = fopen(name.c_str(), "w");
  name = "62414.txt";
  f_map[62414] = fopen(name.c_str(), "w");
  name = "47802.txt";
  f_map[47802] = fopen(name.c_str(), "w");
  */
  name = "1.txt";
  f_map[1] = fopen(name.c_str(), "w");
  s = trace_reader_->ReadHeader(header);
  if (!s.ok()) {
    return s;
  }

  Trace footer;
  s = trace_reader_->ReadFooter(footer);
  if (!s.ok()) {
    return s;
  }

  Trace trace;
  while (s.ok()) {
    trace.reset();
    s = trace_reader_->ReadRecord(trace);
    if (!s.ok()) {
      break;
    }

    total_requests_++;
    end_time_ = trace.ts;
    if (begin_time_ == 0) {
      begin_time_ = trace.ts;
    }

    if (trace.type == kTraceWrite) {
      continue;
    } else if (trace.type == kTraceGet) {
      if (trace.ts < begin_time_) {
        time_in_sec = 0;
      } else {
        time_in_sec = (trace.ts - begin_time_) / 1000000;
      }
      if (f_map.find(time_in_sec) != f_map.end()) {
        fprintf(f_map[time_in_sec], "%s\n", trace.payload.c_str());
      }
      total_gets_++;
    } else if (trace.type == kTraceIter) {
      continue;
    }
  }
  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
    //
    return Status::OK();
  }
  return s;
}

// After the trace is processed by StartProcessing, the statistic data
// is stored in the map or other in memory data structures. To get the
// other statistic result such as key size distribution, value size
// distribution, are processed here.
Status TraceAnalyzer::MakeStatistics() {
  int ret;
  Status s;
  for (int type = 0; type < taTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto i = ta_[type].stats.begin(); i != ta_[type].stats.end(); i++) {
      i->second.akey_id = 0;
      for (auto it = i->second.a_key_stats.begin();
           it != i->second.a_key_stats.end(); it++) {
        it->second.key_id = i->second.akey_id;
        i->second.akey_id++;
        if (it->second.access_count <= analyzer_opts_.output_ignore_count) {
          continue;
        }

        if (analyzer_opts_.output_access_count_stats) {
          if (i->second.a_count_stats.find(it->second.access_count) ==
              i->second.a_count_stats.end()) {
            i->second.a_count_stats[it->second.access_count] = 1;
          } else {
            i->second.a_count_stats[it->second.access_count]++;
          }
        }

        if (analyzer_opts_.print_key_distribution) {
          if (i->second.a_key_size_stats.find(it->first.size()) ==
              i->second.a_key_size_stats.end()) {
            i->second.a_key_size_stats[it->first.size()] = 1;
          } else {
            i->second.a_key_size_stats[it->first.size()]++;
          }
        }

        if (analyzer_opts_.output_correlation) {
          s = MakeStatisticCorrelation(i->second, it->second);
        }
      }

      // Output the prefix cut or the whole content of the accessed key space
      if (analyzer_opts_.output_key_stats || analyzer_opts_.output_prefix_cut) {
        s = MakeStatisticKeyStatsOrPrefix(i->second);
        if (!s.ok()) {
          return s;
        }
      }

      // output the access count distribution
      if (analyzer_opts_.output_access_count_stats &&
          i->second.a_count_dist_f) {
        for (auto it = i->second.a_count_stats.begin();
             it != i->second.a_count_stats.end(); it++) {
          ret = sprintf(buffer_, "access_count: %" PRIu64 " num: %" PRIu64 "\n",
                        it->first, it->second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = i->second.a_count_dist_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write access count distribution file failed\n");
            return s;
          }
        }
      }

      // find the medium of the key size
      uint64_t k_count = 0;
      for (auto it = i->second.a_key_size_stats.begin();
           it != i->second.a_key_size_stats.end(); it++) {
        k_count += it->second;
        if (k_count >= i->second.a_key_mid) {
          i->second.a_key_mid = it->first;
          break;
        }
      }

      // output the value size distribution
      uint64_t v_begin = 0, v_end = 0, v_count = 0;
      bool get_mid = false;
      for (auto it = i->second.a_value_size_stats.begin();
           it != i->second.a_value_size_stats.end(); it++) {
        v_begin = v_end;
        v_end = (it->first + 1) * analyzer_opts_.value_interval;
        v_count += it->second;
        if (!get_mid && v_count >= i->second.a_count / 2) {
          i->second.a_value_mid = (v_begin + v_end) / 2;
          get_mid = true;
        }
        if (analyzer_opts_.print_value_distribution &&
            i->second.a_value_size_f) {
          ret = sprintf(buffer_,
                        "Number_of_value_size_between %" PRIu64 " and %" PRIu64
                        " is: %" PRIu64 "\n",
                        v_begin, v_end, it->second);
          if (ret < 0) {
            return Status::IOError("Format output failed");
          }
          std::string printout(buffer_);
          s = i->second.a_value_size_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write value size distribution file failed\n");
            return s;
          }
        }
      }
    }
  }

  if (analyzer_opts_.output_io_stats) {
    s = MakeStatisticIO();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

// Process the statistics of the key access and
// prefix of the accessed keys if required
Status TraceAnalyzer::MakeStatisticKeyStatsOrPrefix(TraceStats& stats) {
  int ret;
  Status s;
  std::string prefix = "0";
  uint64_t prefix_access = 0;
  uint64_t prefix_count = 0;
  uint64_t prefix_succ_access = 0;
  double prefix_ave_access = 0.0;
  stats.a_succ_count = 0;
  for (auto it = stats.a_key_stats.begin(); it != stats.a_key_stats.end();
       it++) {
    // write the key access statistic file
    if (!stats.a_key_f) {
      fprintf(stderr, "The accessed_key_stats file is not opend\n");
      exit(1);
    }
    stats.a_succ_count += it->second.succ_count;
    double succ_ratio =
        (static_cast<double>(it->second.succ_count)) / it->second.access_count;
    ret = sprintf(buffer_, "%u %zu %" PRIu64 " %" PRIu64 " %f\n",
                  it->second.cf_id, it->second.value_size, it->second.key_id,
                  it->second.access_count, succ_ratio);
    if (ret < 0) {
      return Status::IOError("Format output failed");
    }
    std::string printout(buffer_);
    s = stats.a_key_f->Append(printout);
    if (!s.ok()) {
      fprintf(stderr, "Write key access file failed\n");
      return s;
    }

    // write the prefix cut of the accessed keys
    if (analyzer_opts_.output_prefix_cut && stats.a_prefix_cut_f) {
      if (it->first.compare(0, analyzer_opts_.prefix_cut, prefix) != 0) {
        std::string prefix_out = rocksdb::LDBCommand::StringToHex(prefix);
        if (prefix_count == 0) {
          prefix_ave_access = 0.0;
        } else {
          prefix_ave_access =
              (static_cast<double>(prefix_access)) / prefix_count;
        }
        double prefix_succ_ratio =
            (static_cast<double>(prefix_succ_access)) / prefix_access;
        ret = sprintf(buffer_, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %f %f %s\n",
                      it->second.key_id, prefix_access, prefix_count,
                      prefix_ave_access, prefix_succ_ratio, prefix_out.c_str());
        if (ret < 0) {
          return Status::IOError("Format output failed");
        }
        std::string pout(buffer_);
        s = stats.a_prefix_cut_f->Append(pout);
        if (!s.ok()) {
          fprintf(stderr, "Write accessed key prefix file failed\n");
          return s;
        }

        // make the top k statistic for the prefix
        if (static_cast<int>(stats.top_k_prefix_access.size()) <
            analyzer_opts_.top_k) {
          stats.top_k_prefix_access.push(
              std::make_pair(prefix_access, prefix_out));
        } else {
          if (prefix_access > stats.top_k_prefix_access.top().first) {
            stats.top_k_prefix_access.pop();
            stats.top_k_prefix_access.push(
                std::make_pair(prefix_access, prefix_out));
          }
        }

        if (static_cast<int>(stats.top_k_prefix_ave.size()) <
            analyzer_opts_.top_k) {
          stats.top_k_prefix_ave.push(
              std::make_pair(prefix_ave_access, prefix_out));
        } else {
          if (prefix_ave_access > stats.top_k_prefix_ave.top().first) {
            stats.top_k_prefix_ave.pop();
            stats.top_k_prefix_ave.push(
                std::make_pair(prefix_ave_access, prefix_out));
          }
        }

        prefix = it->first.substr(0, analyzer_opts_.prefix_cut);
        prefix_access = 0;
        prefix_count = 0;
        prefix_succ_access = 0;
      }
      prefix_access += it->second.access_count;
      prefix_count += 1;
      prefix_succ_access += it->second.succ_count;
    }
  }
  return Status::OK();
}

// Process the statistics of different operation type
// correlations
Status TraceAnalyzer::MakeStatisticCorrelation(TraceStats& stats,
                                               StatsUnit& unit) {
  if (stats.corre_output.size() != analyzer_opts_.corre_list.size()) {
    fprintf(stderr, "Cannot make the statistic of correlation\n");
    return Status::OK();
  }

  for (int i = 0; i < static_cast<int>(analyzer_opts_.corre_list.size()); i++) {
    stats.corre_output[i].first += unit.v_corre[i].count;
    stats.corre_output[i].second += unit.v_corre[i].total_ts;
  }
  return Status::OK();
}

// Process the statistics of IO
Status TraceAnalyzer::MakeStatisticIO() {
  uint32_t duration = (end_time_ - begin_time_) / 1000000;
  int ret;
  Status s;
  std::vector<std::vector<uint32_t>> type_io(
      duration, std::vector<uint32_t>(taTypeNum + 1, 0));
  std::vector<uint64_t> io_sum(taTypeNum + 1, 0);
  std::vector<uint32_t> io_peak(taTypeNum + 1, 0);
  io_ave_.resize(taTypeNum + 1);

  for (int type = 0; type < taTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto i = ta_[type].stats.begin(); i != ta_[type].stats.end(); i++) {
      uint32_t time_line = 0;
      uint64_t cf_io_sum = 0;
      for (auto time_it = i->second.a_io_stats.begin();
           time_it != i->second.a_io_stats.end(); time_it++) {
        if (time_it->first >= duration) {
          continue;
        }
        type_io[time_it->first][taTypeNum] += time_it->second;
        type_io[time_it->first][type] += time_it->second;
        cf_io_sum += time_it->second;
        if (time_it->second > i->second.a_peak_io) {
          i->second.a_peak_io = time_it->second;
        }
        if (i->second.a_io_f) {
          while (time_line < time_it->first) {
            ret = sprintf(buffer_, "%u\n", 0);
            if (ret < 0) {
              return Status::IOError("Format the output failed");
            }
            std::string printout(buffer_);
            s = i->second.a_io_f->Append(printout);
            if (!s.ok()) {
              fprintf(stderr, "Write QPS file failed\n");
              return s;
            }
            time_line++;
          }
          ret = sprintf(buffer_, "%u\n", time_it->second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = i->second.a_io_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write QPS file failed\n");
            return s;
          }
          if (time_line == time_it->first) {
            time_line++;
          }
        }

        // Process the top k IO peaks
        if (analyzer_opts_.output_prefix_cut) {
          if (static_cast<int>(i->second.top_k_io_sec.size()) <
              analyzer_opts_.top_k) {
            i->second.top_k_io_sec.push(
                std::make_pair(time_it->second, time_it->first));
          } else {
            if (i->second.top_k_io_sec.size() > 0 &&
                i->second.top_k_io_sec.top().first < time_it->second) {
              i->second.top_k_io_sec.pop();
              i->second.top_k_io_sec.push(
                  std::make_pair(time_it->second, time_it->first));
            }
          }
        }
      }
      i->second.a_ave_io = (static_cast<double>(cf_io_sum)) / duration;

      // output the prefix of top k access peak
      if (analyzer_opts_.output_prefix_cut && i->second.a_top_io_prefix_f) {
        while (!i->second.top_k_io_sec.empty()) {
          ret = sprintf(buffer_, "At time: %u with IO num: %u\n",
                        i->second.top_k_io_sec.top().second,
                        i->second.top_k_io_sec.top().first);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = i->second.a_top_io_prefix_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write prefix QPS top K file failed\n");
            return s;
          }
          uint32_t io_time = i->second.top_k_io_sec.top().second;
          i->second.top_k_io_sec.pop();
          if (i->second.a_io_prefix_stats.find(io_time) !=
              i->second.a_io_prefix_stats.end()) {
            for (auto io_prefix = i->second.a_io_prefix_stats[io_time].begin();
                 io_prefix != i->second.a_io_prefix_stats[io_time].end();
                 io_prefix++) {
              std::string io_prefix_out =
                  rocksdb::LDBCommand::StringToHex(io_prefix->first);
              ret = sprintf(buffer_, "The prefix: %s Access count: %u\n",
                            io_prefix_out.c_str(), io_prefix->second);
              if (ret < 0) {
                return Status::IOError("Format the output failed");
              }
              std::string pout(buffer_);
              s = i->second.a_top_io_prefix_f->Append(pout);
              if (!s.ok()) {
                fprintf(stderr, "Write prefix QPS top K file failed\n");
                return s;
              }
            }
          }
        }
      }
    }
  }

  if (qps_f_) {
    for (uint32_t i = 0; i < duration; i++) {
      for (int type = 0; type <= taTypeNum; type++) {
        if (type < taTypeNum) {
          ret = sprintf(buffer_, "%u ", type_io[i][type]);
        } else {
          ret = sprintf(buffer_, "%u\n", type_io[i][type]);
        }
        if (ret < 0) {
          return Status::IOError("Format the output failed");
        }
        std::string printout(buffer_);
        s = qps_f_->Append(printout);
        if (!s.ok()) {
          return s;
        }
        io_sum[type] += type_io[i][type];
        if (type_io[i][type] > io_peak[type]) {
          io_peak[type] = type_io[i][type];
        }
      }
    }
  }

  io_peak_ = io_peak;
  for (int type = 0; type <= taTypeNum; type++) {
    io_ave_[type] = (static_cast<double>(io_sum[type])) / duration;
  }

  return Status::OK();
}

// In reprocessing, if we have the whole key space
// we can output the access count of all keys in a cf
// we can make some statistics of the whole key space
// also, we output the top k accessed keys here
//
Status TraceAnalyzer::ReProcessing() {
  int ret;
  Status s;
  for (auto cf_it = cfs_.begin(); cf_it != cfs_.end(); cf_it++) {
    uint32_t cf_id = cf_it->first;

    // output the time serial;
    if (analyzer_opts_.output_time_serial) {
      for (int i = 0; i < taTypeNum; i++) {
        if (!ta_[i].enabled || ta_[i].stats.find(cf_id) == ta_[i].stats.end()) {
          continue;
        }
        TraceStats& stats = ta_[i].stats[cf_id];
        if (!stats.time_serial_f) {
          fprintf(stderr, "Cannot write time_serial of '%s' in '%u'\n",
                  ta_[i].type_name.c_str(), cf_id);
          continue;
        }
        while (!stats.time_serial.empty()) {
          uint64_t key_id = 0;
          auto found = stats.a_key_stats.find(stats.time_serial.front().key);
          if (found != stats.a_key_stats.end()) {
            key_id = found->second.key_id;
          }
          ret = sprintf(buffer_, "%u %" PRIu64 " %" PRIu64 "\n",
                        stats.time_serial.front().type,
                        stats.time_serial.front().ts, key_id);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          s = stats.time_serial_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write time series file failed\n");
            return s;
          }
          stats.time_serial.pop_front();
        }
      }
    }

    // process the whole key space if needed
    if (analyzer_opts_.input_key_space) {
      std::string whole_key_path =
          analyzer_opts_.key_space_dir + "/" + std::to_string(cf_id) + ".txt";
      std::string input_key, get_key;
      std::vector<std::string> prefix(taTypeNum);
      std::istringstream iss;
      bool has_data = true;
      s = env_->NewSequentialFile(whole_key_path, &wkey_input_f_, env_options_);
      if (!s.ok()) {
        fprintf(stderr, "Cannot open the whole key space file of CF: %u\n", cf_id);
        wkey_input_f_.release();
      }
      if (wkey_input_f_) {
        for(cfs_[cf_id].w_count = 0; ReadOneLine(&iss, wkey_input_f_.get(), &get_key, &has_data, &s); ++cfs_[cf_id].w_count) {
          if (!s.ok()) {
            fprintf(stderr, "Read whole key space file failed\n");
            return s;
          }

          input_key = rocksdb::LDBCommand::HexToString(get_key);
          for (int i = 0; i < taTypeNum; i++) {
            if (!ta_[i].enabled) {
              continue;
            }
            TraceStats& stats = ta_[i].stats[cf_id];
            if (stats.w_key_f) {
              if (stats.a_key_stats.find(input_key) !=
                  stats.a_key_stats.end()) {
                ret = sprintf(buffer_, "%" PRIu64 " %" PRIu64 "\n",
                              cfs_[cf_id].w_count,
                              stats.a_key_stats[input_key].access_count);
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stats.w_key_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr, "Write whole key space access file failed\n");
                  return s;
                }
              }
            }

            // Output the prefix cut file of the whole key space
            if (analyzer_opts_.output_prefix_cut && stats.w_prefix_cut_f) {
              if (input_key.compare(0, analyzer_opts_.prefix_cut, prefix[i]) !=
                  0) {
                prefix[i] = input_key.substr(0, analyzer_opts_.prefix_cut);
                std::string prefix_out =
                    rocksdb::LDBCommand::StringToHex(prefix[i]);
                ret = sprintf(buffer_, "%" PRIu64 " %s\n", cfs_[cf_id].w_count,
                              prefix_out.c_str());
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stats.w_prefix_cut_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr,
                          "Write whole key space prefix cut file failed\n");
                  return s;
                }
              }
            }
          }

          // Make the statistics fo the key size distribution
          if (analyzer_opts_.print_key_distribution) {
            if (cfs_[cf_id].w_key_size_stats.find(input_key.size()) ==
                cfs_[cf_id].w_key_size_stats.end()) {
              cfs_[cf_id].w_key_size_stats[input_key.size()] = 1;
            } else {
              cfs_[cf_id].w_key_size_stats[input_key.size()]++;
            }
          }

        }
      }
    }

    // process the top k accessed keys
    if (analyzer_opts_.print_top_k_access) {
      for (int i = 0; i < taTypeNum; i++) {
        if (!ta_[i].enabled || ta_[i].stats.find(cf_id) == ta_[i].stats.end()) {
          continue;
        }
        TraceStats& stats = ta_[i].stats[cf_id];
        for (auto it = stats.a_key_stats.begin(); it != stats.a_key_stats.end();
             it++) {
          if (static_cast<int>(stats.top_k_queue.size()) <
              analyzer_opts_.top_k) {
            stats.top_k_queue.push(
                std::make_pair(it->second.access_count, it->first));
          } else {
            if (it->second.access_count > stats.top_k_queue.top().first) {
              stats.top_k_queue.pop();
              stats.top_k_queue.push(
                  std::make_pair(it->second.access_count, it->first));
            }
          }
        }
      }
    }
  }
  return Status::OK();
}

// End the processing, print the requested results
Status TraceAnalyzer::EndProcessing() {
  if (trace_sequence_f_) {
    trace_sequence_f_->Close();
  }
  PrintGetStatistics();
  CloseOutputFiles();
  return Status::OK();
}

// Insert the corresponding key statistics to the correct type
// and correct CF, output the time-serial file if needed
Status TraceAnalyzer::KeyStatsInsertion(const uint32_t& type,
                                        const uint32_t& cf_id,
                                        const std::string& key,
                                        const size_t value_size,
                                        const uint64_t ts) {
  auto found_stats = ta_[type].stats.find(cf_id);
  Status s;
  StatsUnit unit;
  unit.key_id = 0;
  unit.cf_id = cf_id;
  unit.value_size = value_size;
  unit.access_count = 1;
  unit.latest_ts = ts;
  if (type != taGet || value_size > 0) {
    unit.succ_count = 1;
  } else {
    unit.succ_count = 0;
  }
  unit.v_corre.resize(analyzer_opts_.corre_list.size());
  for (int i = 0; i < (static_cast<int>(analyzer_opts_.corre_list.size())); i++) {
    unit.v_corre[i].count = 0;
    unit.v_corre[i].total_ts = 0;
  }
  std::string prefix;
  if (analyzer_opts_.output_prefix_cut) {
    prefix = key.substr(0, analyzer_opts_.prefix_cut);
  }

  if (begin_time_ == 0) {
    begin_time_ = ts;
  }
  uint32_t time_in_sec;
  if (ts < begin_time_) {
    time_in_sec = 0;
  } else {
    time_in_sec = (ts - begin_time_) / 1000000;
  }
  uint64_t dist_value_size = value_size / analyzer_opts_.value_interval;
  if (found_stats == ta_[type].stats.end()) {
    ta_[type].stats[cf_id].cf_id = cf_id;
    ta_[type].stats[cf_id].cf_name = std::to_string(cf_id);
    ta_[type].stats[cf_id].a_count = 1;
    ta_[type].stats[cf_id].akey_id = 0;
    ta_[type].stats[cf_id].a_key_size_sqsum = key.size() * key.size();
    ta_[type].stats[cf_id].a_key_size_sum = key.size();
    ta_[type].stats[cf_id].a_value_size_sqsum = value_size * value_size;
    ta_[type].stats[cf_id].a_value_size_sum = value_size;
    s = OpenStatsOutputFiles(ta_[type].type_name, ta_[type].stats[cf_id]);
    if (analyzer_opts_.output_correlation) {
      s = StatsUnitCorreUpdate(unit, type, ts, key);
    }
    ta_[type].stats[cf_id].a_key_stats[key] = unit;
    ta_[type].stats[cf_id].a_value_size_stats[dist_value_size] = 1;
    ta_[type].stats[cf_id].a_io_stats[time_in_sec] = 1;
    ta_[type].stats[cf_id].corre_output.resize(analyzer_opts_.corre_list.size());
    if (analyzer_opts_.output_prefix_cut) {
      std::map<std::string, uint32_t> tmp_io_map;
      tmp_io_map[prefix] = 1;
      ta_[type].stats[cf_id].a_io_prefix_stats[time_in_sec] = tmp_io_map;
    }
    //ta_[type].stats[cf_id] = std::move(ta_[type].stats[cf_id]);
  } else {
    found_stats->second.a_count++;
    found_stats->second.a_key_size_sqsum += key.size() * key.size();
    found_stats->second.a_key_size_sum += key.size();
    found_stats->second.a_value_size_sqsum += value_size * value_size;
    found_stats->second.a_value_size_sum += value_size;
    auto found_key = found_stats->second.a_key_stats.find(key);
    if (found_key == found_stats->second.a_key_stats.end()) {
      found_stats->second.a_key_stats[key] = unit;
    } else {
      found_key->second.access_count++;
      if (type != taGet || value_size > 0) {
        found_key->second.succ_count++;
      }
      if (analyzer_opts_.output_correlation) {
        s = StatsUnitCorreUpdate(found_key->second, type, ts, key);
      }
    }

    auto found_value =
        found_stats->second.a_value_size_stats.find(dist_value_size);
    if (found_value == found_stats->second.a_value_size_stats.end()) {
      found_stats->second.a_value_size_stats[dist_value_size] = 1;
    } else {
      found_value->second++;
    }

    auto found_io = found_stats->second.a_io_stats.find(time_in_sec);
    if (found_io == found_stats->second.a_io_stats.end()) {
      found_stats->second.a_io_stats[time_in_sec] = 1;
    } else {
      found_io->second++;
    }

    if (analyzer_opts_.output_prefix_cut) {
      auto found_io_prefix =
          found_stats->second.a_io_prefix_stats.find(time_in_sec);
      if (found_io_prefix == found_stats->second.a_io_prefix_stats.end()) {
        std::map<std::string, uint32_t> tmp_io_map;
        found_stats->second.a_io_prefix_stats[time_in_sec] = tmp_io_map;
      }
      if (found_stats->second.a_io_prefix_stats[time_in_sec].find(prefix) ==
          found_stats->second.a_io_prefix_stats[time_in_sec].end()) {
        found_stats->second.a_io_prefix_stats[time_in_sec][prefix] = 1;
      } else {
        found_stats->second.a_io_prefix_stats[time_in_sec][prefix]++;
      }
    }
  }

  if (cfs_.find(cf_id) == cfs_.end()) {
    CfUnit cf_unit;
    cf_unit.cf_id = cf_id;
    cf_unit.w_count = 0;
    cf_unit.a_count = 0;
    cfs_[cf_id] = cf_unit;
  }

  if (analyzer_opts_.output_time_serial) {
    TraceUnit trace_u;
    trace_u.type = type;
    trace_u.key = key;
    trace_u.value_size = value_size;
    trace_u.ts = (ts - analyzer_opts_.start_time) / 1000000;
    trace_u.cf_id = cf_id;
    ta_[type].stats[cf_id].time_serial.push_back(trace_u);
  }

  return Status::OK();
}

Status TraceAnalyzer::StatsUnitCorreUpdate(StatsUnit& unit,
                                           const uint32_t& type,
                                           const uint64_t& ts,
                                           const std::string& key) {
  if (type >= taTypeNum) {
    fprintf(stderr, "Unknown Type Id: %u\n", type);
    exit(1);
  }

  for (int i = 0; i<taTypeNum; i++) {
    if (analyzer_opts_.corre_map[i][type] < 0 ||
        ta_[i].stats.find(unit.cf_id) == ta_[i].stats.end()||
        ta_[i].stats[unit.cf_id].a_key_stats.find(key) ==
        ta_[i].stats[unit.cf_id].a_key_stats.end() ||
        ta_[i].stats[unit.cf_id].a_key_stats[key].latest_ts == ts) {
      continue;
    }

    int corre_id = analyzer_opts_.corre_map[i][type];

    //after get the x-y operation time or x, update;
    unit.v_corre[corre_id].count++;
    unit.v_corre[corre_id].total_ts += (ts -
        ta_[i].stats[unit.cf_id].a_key_stats[key].latest_ts);
  }

  unit.latest_ts = ts;
  return Status::OK();
}

// when a new trace stattistic is created, the file handler
// pointers should be initiated if needed according to
// the trace analyzer options
Status TraceAnalyzer::OpenStatsOutputFiles(const std::string& type,
                                           TraceStats& new_stats) {
  Status s;
  if (analyzer_opts_.output_key_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "accessed_key_stats.txt",
                         &new_stats.a_key_f);
    if (analyzer_opts_.input_key_space) {
      s = CreateOutputFile(type, new_stats.cf_name, "whole_key_stats.txt",
                           &new_stats.w_key_f);
    }
  }

  if (analyzer_opts_.output_access_count_stats) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_key_count_distribution.txt",
                         &new_stats.a_count_dist_f);
  }

  if (analyzer_opts_.output_prefix_cut) {
    s = CreateOutputFile(type, new_stats.cf_name, "accessed_key_prefix_cut.txt",
                         &new_stats.a_prefix_cut_f);
    if (analyzer_opts_.input_key_space) {
      s = CreateOutputFile(type, new_stats.cf_name, "whole_key_prefix_cut.txt",
                           &new_stats.w_prefix_cut_f);
    }

    if (analyzer_opts_.output_io_stats) {
      s = CreateOutputFile(type, new_stats.cf_name,
                           "accessed_top_k_io_prefix_cut.txt",
                           &new_stats.a_top_io_prefix_f);
    }
  }

  if (analyzer_opts_.output_time_serial) {
    s = CreateOutputFile(type, new_stats.cf_name, "time_serial.txt",
                         &new_stats.time_serial_f);
  }

  if (analyzer_opts_.print_value_distribution) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_value_size_distribution.txt",
                         &new_stats.a_value_size_f);
  }

  if (analyzer_opts_.output_io_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "io_stats.txt",
                         &new_stats.a_io_f);
  }

  return Status::OK();
}

// create the output path of the files to be opened
Status TraceAnalyzer::CreateOutputFile(const std::string& type, const std::string& cf_name,
                        const std::string& ending,
                        std::unique_ptr<rocksdb::WritableFile>* f_ptr) {
  std::string path;
  path = output_path_ + "/" + analyzer_opts_.output_prefix + "-" + type + "-" +
         cf_name + "-" + ending;
  Status s;
  s = env_->NewWritableFile(path, f_ptr, env_options_);
  if (!s.ok()) {
    fprintf(stderr, "Cannot open file: %s\n", path.c_str());
    exit(1);
  }
  return Status::OK();
}

// Close the output files in the TraceStats if they are opened
void TraceAnalyzer::CloseOutputFiles() {
  for (int type = 0; type < taTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto i = ta_[type].stats.begin(); i != ta_[type].stats.end(); i++) {
      if (i->second.time_serial_f) {
        i->second.time_serial_f->Close();
      }

      if (i->second.a_key_f) {
        i->second.a_key_f->Close();
      }

      if (i->second.a_count_dist_f) {
        i->second.a_count_dist_f->Close();
      }

      if (i->second.a_prefix_cut_f) {
        i->second.a_prefix_cut_f->Close();
      }

      if (i->second.a_value_size_f) {
        i->second.a_value_size_f->Close();
      }

      if (i->second.a_io_f) {
        i->second.a_io_f->Close();
      }

      if (i->second.a_top_io_prefix_f) {
        i->second.a_top_io_prefix_f->Close();
      }

      if (i->second.w_key_f) {
        i->second.w_key_f->Close();
      }
      if (i->second.w_prefix_cut_f) {
        i->second.w_prefix_cut_f->Close();
      }
    }
  }
  return;
}

// Handle the Get request in the trace
Status TraceAnalyzer::HandleGetCF(uint32_t column_family_id,
                                  const std::string& key, const uint64_t& ts,
                                  const uint32_t& get_ret) {
  Status s;
  size_t value_size = 0;
  if (analyzer_opts_.output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(taGet, column_family_id, key, value_size, ts);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[taGet].enabled) {
    return Status::OK();
  }
  if (get_ret == 1) {
    value_size = 10;
  }
  s = KeyStatsInsertion(taGet, column_family_id, key, value_size, ts);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Put request in the write batch of the trace
Status TraceAnalyzer::HandlePutCF(uint32_t column_family_id, const Slice& key,
                                  const Slice& value) {
  Status s;
  size_t value_size = value.ToString().size();
  if (analyzer_opts_.output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(taPut, column_family_id, key.ToString(), value_size,
                           c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[taPut].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(taPut, column_family_id, key.ToString(), value_size,
                        c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Delete request in the write batch of the trace
Status TraceAnalyzer::HandleDeleteCF(uint32_t column_family_id,
                                     const Slice& key) {
  Status s;
  size_t value_size = 0;
  if (analyzer_opts_.output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(taDelete, column_family_id, key.ToString(),
                           value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[taDelete].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(taDelete, column_family_id, key.ToString(), value_size,
                        c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the SingleDelete request in the write batch of the trace
Status TraceAnalyzer::HandleSingleDeleteCF(uint32_t column_family_id,
                                           const Slice& key) {
  Status s;
  size_t value_size = 0;
  if (analyzer_opts_.output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(taSingleDelete, column_family_id, key.ToString(),
                           value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[taSingleDelete].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(taSingleDelete, column_family_id, key.ToString(),
                        value_size, c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the DeleteRange request in the write batch of the trace
Status TraceAnalyzer::HandleDeleteRangeCF(uint32_t column_family_id,
                                          const Slice& begin_key,
                                          const Slice& end_key) {
  Status s;
  size_t value_size = 0;
  if (analyzer_opts_.output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(taRangeDelete, column_family_id,
                           begin_key.ToString(), value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[taRangeDelete].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(taRangeDelete, column_family_id, begin_key.ToString(),
                        value_size, c_time_);
  s = KeyStatsInsertion(taRangeDelete, column_family_id, end_key.ToString(),
                        value_size, c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Merge request in the write batch of the trace
Status TraceAnalyzer::HandleMergeCF(uint32_t column_family_id, const Slice& key,
                                    const Slice& value) {
  Status s;
  size_t value_size = value.ToString().size();
  if (analyzer_opts_.output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(taMerge, column_family_id, key.ToString(),
                           value_size, c_time_);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[taMerge].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(taMerge, column_family_id, key.ToString(), value_size,
                        c_time_);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Handle the Iterator request in the trace
Status TraceAnalyzer::HandleIterCF(uint32_t column_family_id,
                                   const std::string& key, const uint64_t& ts) {
  Status s;
  size_t value_size = 0;
  if (analyzer_opts_.output_trace_sequence && trace_sequence_f_) {
    s = WriteTraceSequence(taIter, column_family_id, key, value_size, ts);
    if (!s.ok()) {
      return Status::Corruption("Failed to write the trace sequence to file");
    }
  }

  if (!ta_[taIter].enabled) {
    return Status::OK();
  }
  s = KeyStatsInsertion(taIter, column_family_id, key, value_size, ts);
  if (!s.ok()) {
    return Status::Corruption("Failed to insert key statistics");
  }
  return s;
}

// Before the analyzer is closed, the requested general statistic results are
// printed out here. In current stage, these information are not output to
// the files.
// -----type
//          |__cf_id
//                |_statistics
void TraceAnalyzer::PrintGetStatistics() {
  for (int type = 0; type < taTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    ta_[type].total_keys = 0;
    ta_[type].total_access = 0;
    ta_[type].total_succ_access = 0;
    printf("\n################# Operation Type: %s #####################\n",
           ta_[type].type_name.c_str());
    printf("Peak IO is: %u Average IO is: %f\n", io_peak_[type], io_ave_[type]);
    for (auto i = ta_[type].stats.begin(); i != ta_[type].stats.end(); i++) {
      if (i->second.a_count == 0) {
        continue;
      }
      TraceStats& stats = i->second;

      uint64_t total_a_keys = static_cast<uint64_t>(stats.a_key_stats.size());
      double key_size_ave =
          (static_cast<double>(stats.a_key_size_sum)) / stats.a_count;
      double value_size_ave =
          (static_cast<double>(stats.a_value_size_sum)) / stats.a_count;
      double key_size_vari =
          sqrt((static_cast<double>(stats.a_key_size_sqsum)) / stats.a_count -
               key_size_ave * key_size_ave);
      double value_size_vari =
          sqrt((static_cast<double>(stats.a_value_size_sqsum)) / stats.a_count -
               value_size_ave * value_size_ave);
      if (value_size_ave == 0.0) {
        stats.a_value_mid = 0;
      }
      cfs_[stats.cf_id].a_count += total_a_keys;
      ta_[type].total_keys += total_a_keys;
      ta_[type].total_access += stats.a_count;
      ta_[type].total_succ_access += stats.a_succ_count;
      double cf_succ_ratio =
          (static_cast<double>(stats.a_succ_count)) / stats.a_count;
      printf("*********************************************************\n");
      printf("colume family id: %u\n", stats.cf_id);
      printf("Total unique keys in this cf: %" PRIu64 "\n", total_a_keys);
      printf("Total '%s' requests on cf '%u': %" PRIu64 " succssed: %" PRIu64
             " ratio: %f\n",
             ta_[type].type_name.c_str(), stats.cf_id, stats.a_count,
             stats.a_succ_count, cf_succ_ratio);
      printf("Average key size: %f key size medium: %" PRIu64
             " Key size Variation: %f\n",
             key_size_ave, stats.a_key_mid, key_size_vari);
      printf("Average value size: %f Value size medium: %" PRIu64
             " Value size variation: %f\n",
             value_size_ave, stats.a_value_mid, value_size_vari);
      printf("Peak IO is: %u Average IO is: %f\n", stats.a_peak_io,
             stats.a_ave_io);

      // print the top k accessed key and its access count
      if (analyzer_opts_.print_top_k_access) {
        printf("The Top %d keys that are accessed:\n", analyzer_opts_.top_k);
        while (!stats.top_k_queue.empty()) {
          std::string hex_key =
              rocksdb::LDBCommand::StringToHex(stats.top_k_queue.top().second);
          double succ_ratio =
              (static_cast<double>(
                  stats.a_key_stats[stats.top_k_queue.top().second]
                      .succ_count)) /
              stats.a_key_stats[stats.top_k_queue.top().second].access_count;
          printf("Access_count: %" PRIu64 " Successed: %" PRIu64
                 " Ratio: %f %s\n",
                 stats.top_k_queue.top().first,
                 stats.a_key_stats[stats.top_k_queue.top().second].succ_count,
                 succ_ratio, hex_key.c_str());
          stats.top_k_queue.pop();
        }
      }

      // print the top k access prefix range and
      // top k prefix range with highest average access per key
      if (analyzer_opts_.output_prefix_cut) {
        printf("The Top %d accessed prefix range:\n", analyzer_opts_.top_k);
        while (!stats.top_k_prefix_access.empty()) {
          printf("Prefix: %s Access count: %" PRIu64 "\n",
                 stats.top_k_prefix_access.top().second.c_str(),
                 stats.top_k_prefix_access.top().first);
          stats.top_k_prefix_access.pop();
        }

        printf("The Top %d prefix with highest access per key:\n",
               analyzer_opts_.top_k);
        while (!stats.top_k_prefix_ave.empty()) {
          printf("Prefix: %s access per key: %f\n",
                 stats.top_k_prefix_ave.top().second.c_str(),
                 stats.top_k_prefix_ave.top().first);
          stats.top_k_prefix_ave.pop();
        }
      }

      // print the key size distribution
      if (analyzer_opts_.print_key_distribution) {
        printf("The key size distribution\n");
        for (auto it = stats.a_key_size_stats.begin();
             it != stats.a_key_size_stats.end(); it++) {
          printf("key_size %" PRIu64 " nums: %" PRIu64 "\n", it->first,
                 it->second);
        }
      }

      // print the operation correlations
      if (analyzer_opts_.output_correlation) {
        for (int corre = 0;
             corre < static_cast<int>(analyzer_opts_.corre_list.size());
             corre++) {
          printf("The correlation statistics of '%s' after '%s' is:",
                 taIndexToOpt[analyzer_opts_.corre_list[corre].second].c_str(),
                 taIndexToOpt[analyzer_opts_.corre_list[corre].first].c_str());
          double corre_ave =
              (static_cast<double>(stats.corre_output[corre].second)) /
              (stats.corre_output[corre].first * 1000);
          printf(" total numbers: %" PRIu64 " average time: %f(ms)\n",
                 stats.corre_output[corre].first, corre_ave);
        }
      }
    }
    printf("*********************************************************\n");
    printf("Total keys of '%s' is: %" PRIu64 "\n", ta_[type].type_name.c_str(),
           ta_[type].total_keys);
    double type_succ_ratio =
        (static_cast<double>(ta_[type].total_succ_access)) /
        ta_[type].total_access;
    printf("Total access is: %" PRIu64 " Successed: %" PRIu64 " Ratio: %f\n",
           ta_[type].total_access, ta_[type].total_succ_access,
           type_succ_ratio);
    total_access_keys_ += ta_[type].total_keys;
  }

  // Print the overall statistic information of the trace
  printf("\n*********************************************************\n");
  printf("*********************************************************\n");
  printf("The column family based statistics\n");
  for (auto it = cfs_.begin(); it != cfs_.end(); it++) {
    printf("The column family id: %u\n", it->first);
    printf("The whole key space key numbers: %" PRIu64 "\n",
           it->second.w_count);
    printf("The accessed key space key numbers: %" PRIu64 "\n",
           it->second.a_count);
  }

  if (analyzer_opts_.print_overall_stats) {
    printf("\n*********************************************************\n");
    printf("*********************************************************\n");
    printf("Average IO per second: %f Peak IO: %u\n", io_ave_[taTypeNum],
           io_peak_[taTypeNum]);
    printf("Total_requests: %" PRIu64 " Total_accessed_keys: %" PRIu64
           " Total_gets: %" PRIu64 " Total_write_batch: %" PRIu64 "\n",
           total_requests_, total_access_keys_, total_gets_, total_writes_);
    for (int type = 0; type < taTypeNum; type++) {
      if (!ta_[type].enabled) {
        continue;
      }
      printf("Operation: '%s' has: %" PRIu64 "\n", ta_[type].type_name.c_str(),
             ta_[type].total_access);
    }
  }
}

// Write the trace sequence to file
Status TraceAnalyzer::WriteTraceSequence(const uint32_t& type,
                                         const uint32_t& cf_id,
                                         const std::string& key,
                                         const size_t value_size,
                                         const uint64_t ts) {
  std::string hex_key = rocksdb::LDBCommand::StringToHex(key);
  int ret;
  ret =
      sprintf(buffer_, "%u %u %zu %" PRIu64 "\n", type, cf_id, value_size, ts);
  if (ret < 0) {
    return Status::IOError("failed to write the file");
  }
  std::string printout(buffer_);
  if (!analyzer_opts_.no_key) {
    printout = hex_key + " " + printout;
  }
  return trace_sequence_f_->Append(printout);
}

namespace {

void print_help() {
  fprintf(stderr,
          R"(trace_analyzer --trace_file=<trace file path> [--comman=]
    --trace_file=<trace file path>
      The trace path
    --intput_key_space_dir=<the directory stores full key space files>
      The key space file should be named as <column family name>.txt
    --output_dir=<the output dir>
      The directory to store the output files
    --output_prefix=<the prefix of all output>
      The prefix used for all the output files
    --output_key_stats
      Output the key access count statistics to file
      for accessed keys:
      format:[cf_id value_size access_keyid access_count]
      for whole key space:
      format:[whole_key_space_keyid access_count]
    --output_access_count_stats
      Output the access count distribution statistics to file
      format:[access_count number_of_access_count]
    --output_time_serial=<trace collect time>
      Output the access time sequence of keys with key space of GET
      format:[type_id time_in_sec access_key_id]
    --output_prefix_cut=<# of byte as prefix to cut>
      Output the key space cut point based on the prefix
      for accessed keys:
      format:[acessed_keyid access_count num_keys average_access prefix]
      for whole key space:
      format:[start_keyid_in_whole_keyspace prefix]
      if used with --output_io_stats
      format:[time_in_sec IO_num]
             [prefix access_count_in_this_time_sec]
    --output_trace_sequence
      Out put the trace sequence for further processing
      including the type, cf_id, ts, value_sze, key. This file
      will be extremely large (similar size as the original trace)
      format:[type_id cf_id value_size time_in_micorsec <key>]
    --output_io_stats
      Output the statistics of the IO per second, the IO including all
      operations
      format:[operation_count_in_this_second]
    --output_correlation=<[correlation pairs][.,.]>
      Output the operation correlations between the pairs of operations
      listed in the parameter, input should select the operations from:
      get, put, delete, single_delete, rangle_delete, merge. No space
      between the pairs separated by commar. Example: =[get,get][put,get]
    --use_get
      Analyze the Get operations
    --use_put
      Analyze the Put operations
    --use_delete
      Analyze the Delete operations
    --use_single_delete
      Analyze the SingleDelete operations
    --use_range_delete
      Analyze the DeleteRange operations
    --use_merge
      Analyze the MERGE operations
    --use_iterator
      Analyze the iterate operation like seek() and seekForPre()
    --print_overall_stats
      Print the stats of the whole trace, like total requests, keys, and etc.
    --print_key_distribution
      Print the key size distribution
    --print_value_distribution
      Print the value size distribution, only available for write
    --print_top_k_access=<top K to be printed>
      Print the top k accessed keys, top k accessed prefix, top K QPS, and etc.
    --output_ignore_count=
      ignores the access count <= this value to shorter the output
    --value_interval=
      To output the value distribution, we need to set the value intervals and
      make the statistic of the value size distribution in different intervals
      The default is 128B
    --no_key
      Does not output the key to the result files to make them smaller
 )");
}

}  // namespace

int TraceAnalyzerTool::Run(int argc, char** argv) {
  std::string trace_path;
  std::string output_path;
  struct stat info;

  AnalyzerOptions analyzer_opts;

  if (argc <= 1) {
    print_help();
    exit(1);
  }

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--trace_file=", 13) == 0) {
      trace_path = argv[i] + 13;
      if (stat(trace_path.c_str(), &info) != 0) {
        fprintf(stderr, "Unknown path: %s\n", trace_path.c_str());
        exit(1);
      }
    } else if (strncmp(argv[i], "--output_dir=", 13) == 0) {
      output_path = argv[i] + 13;
      if (stat(output_path.c_str(), &info) != 0) {
        fprintf(stderr, "Unknown path: %s\n", output_path.c_str());
        exit(1);
      }
    } else if (strncmp(argv[i], "--output_prefix=", 16) == 0) {
      analyzer_opts.output_prefix = argv[i] + 16;
    } else if (strncmp(argv[i], "--output_key_stats", 18) == 0) {
      analyzer_opts.output_key_stats = true;
    } else if (strncmp(argv[i], "--output_access_count_stats", 27) == 0) {
      analyzer_opts.output_access_count_stats = true;
    } else if (strncmp(argv[i], "--output_time_serial=", 21) == 0) {
      std::string::size_type sz = 0;
      std::string tmp = argv[i] + 21;
      analyzer_opts.start_time = std::stoull(tmp, &sz, 0);
      analyzer_opts.output_time_serial = true;
    } else if (strncmp(argv[i], "--output_prefix_cut=", 20) == 0) {
      std::string tmp = argv[i] + 20;
      analyzer_opts.prefix_cut = std::stoi(tmp);
      analyzer_opts.output_prefix_cut = true;
    } else if (strncmp(argv[i], "--output_trace_sequence", 23) == 0) {
      analyzer_opts.output_trace_sequence = true;
    } else if (strncmp(argv[i], "--output_io_stats", 17) == 0) {
      analyzer_opts.output_io_stats = true;
    } else if (strncmp(argv[i], "--output_correlation=", 21) == 0) {
      std::string tmp = argv[i] + 21;
      analyzer_opts.SparseCorreInput(tmp);
      analyzer_opts.output_correlation = true;
    } else if (strncmp(argv[i], "--intput_key_space_dir=", 23) == 0) {
      analyzer_opts.key_space_dir = argv[i] + 23;
      if (stat(analyzer_opts.key_space_dir.c_str(), &info) != 0) {
        fprintf(stderr, "Unknown path: %s\n",
                analyzer_opts.key_space_dir.c_str());
        exit(1);
      }
      analyzer_opts.input_key_space = true;
    } else if (strncmp(argv[i], "--use_get", 9) == 0) {
      analyzer_opts.use_get = true;
    } else if (strncmp(argv[i], "--use_put", 9) == 0) {
      analyzer_opts.use_put = true;
    } else if (strncmp(argv[i], "--use_delete", 12) == 0) {
      analyzer_opts.use_delete = true;
    } else if (strncmp(argv[i], "--use_single_delete", 19) == 0) {
      analyzer_opts.use_single_delete = true;
    } else if (strncmp(argv[i], "--use_range_delete", 18) == 0) {
      analyzer_opts.use_range_delete = true;
    } else if (strncmp(argv[i], "--use_merge", 11) == 0) {
      analyzer_opts.use_merge = true;
    } else if (strncmp(argv[i], "--use_iterator", 14) == 0) {
      analyzer_opts.use_iterator = true;
    } else if (strncmp(argv[i], "--no_key", 8) == 0) {
      analyzer_opts.no_key = true;
    } else if (strncmp(argv[i], "--print_overall_stats", 21) == 0) {
      analyzer_opts.print_overall_stats = true;
    } else if (strncmp(argv[i], "--print_key_distribution", 24) == 0) {
      analyzer_opts.print_key_distribution = true;
    } else if (strncmp(argv[i], "--print_value_distribution", 26) == 0) {
      analyzer_opts.print_value_distribution = true;
    } else if (strncmp(argv[i], "--special", 9) == 0) {
      analyzer_opts.special = true;
    } else if (strncmp(argv[i], "--print_top_k_access=", 21) == 0) {
      std::string tmp = argv[i] + 21;
      analyzer_opts.top_k = std::stoi(tmp);
      if (analyzer_opts.top_k < 0 || analyzer_opts.top_k > 50) {
        fprintf(stderr, "Unacceptable top_k value: %d\n", analyzer_opts.top_k);
        exit(1);
      }
      analyzer_opts.print_top_k_access = true;
    } else if (strncmp(argv[i], "--output_ignore_count=", 22) == 0) {
      std::string tmp = argv[i] + 22;
      analyzer_opts.output_ignore_count = std::stoi(tmp);
    } else if (strncmp(argv[i], "--value_interval=", 17) == 0) {
      std::string tmp = argv[i] + 17;
      analyzer_opts.value_interval = std::stoi(tmp);
      if (analyzer_opts.value_interval < 0) {
        fprintf(stderr, "Unacceptable value_interval: %d\n",
                analyzer_opts.value_interval);
        exit(1);
      }
      analyzer_opts.print_value_distribution = true;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help();
      exit(1);
    }
  }

  TraceAnalyzer *analyzer =
      new TraceAnalyzer(trace_path, output_path, analyzer_opts);
  if (analyzer == nullptr) {
    fprintf(stderr, "Cannot initiate the trace analyzer\n");
    exit(1);
  }

  rocksdb::Status s = analyzer->PrepareProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot initiate the trace reader\n");
    delete analyzer;
    exit(1);
  }

  if (analyzer_opts.special) {
    analyzer->SpecialProcessing();
    delete analyzer;
    exit(1);
  }

  s = analyzer->StartProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot processing the trace\n");
    delete analyzer;
    exit(1);
  }

  s = analyzer->MakeStatistics();
  if (!s.ok()) {
    analyzer->EndProcessing();
    fprintf(stderr, "Cannot make the statistics\n");
    delete analyzer;
    exit(1);
  }

  s = analyzer->ReProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot re-process the trace for more statistics\n");
    analyzer->EndProcessing();
    delete analyzer;
    exit(1);
  }

  s = analyzer->EndProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot close the trace analyzer\n");
    delete analyzer;
    exit(1);
  }

  delete analyzer;
  return 0;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
