//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <stdint.h>

#include "db/db_test_util.h"
#include "util/trace_replay.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/trace_analyzer_tool_imp.h"


namespace rocksdb {
namespace {
  static const int kMaxArgCount = 100;
  static const size_t kArgBufferSize = 100000;
}  // namespace


// The helper functions for the test
class TraceAnalyzerTest : public testing::Test {
 public:
  TraceAnalyzerTest() : rnd_(0xFB) {
    test_path_ = test::TmpDir() + "trace_analyzer_test";
    Env::Default()->CreateDir(test_path_);
    dbname_ = test_path_ + "/db";
  }

  ~TraceAnalyzerTest() {

  }

  void GenerateTrace(std::string trace_path) {
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.merge_operator = MergeOperators::CreatePutOperator();
    ReadOptions ro;
    WriteOptions wo;
    TraceOptions trace_opt;
    DB* db_ = nullptr;
    std::string value;

    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_OK(db_->StartTrace(trace_opt, trace_path));

    WriteBatch batch;
    ASSERT_OK(batch.Put("a", "1"));
    ASSERT_OK(batch.Merge("b", "2"));
    ASSERT_OK(batch.Delete("c"));
    ASSERT_OK(batch.SingleDelete("d"));
    ASSERT_OK(batch.DeleteRange("e", "f"));
    ASSERT_OK(db_->Write(wo, &batch));

    ASSERT_OK(db_->Get(ro, "a", &value));
    db_->Get(ro, "g", &value);

    ASSERT_OK(db_->EndTrace(trace_opt));

    ASSERT_OK(Env::Default()->FileExists(trace_path));
  }

  void AppendArgs(const std::vector<std::string>& args) {
    for (const auto& arg : args) {
      ASSERT_LE(cursor_ + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc_ + 1, kMaxArgCount);
      snprintf(arg_buffer_ + cursor_, arg.size() + 1, "%s", arg.c_str());

      argv_[argc_++] = arg_buffer_ + cursor_;
      cursor_ += arg.size() + 1;
    }
  }


  char** argv() { return argv_; }

  int argc() { return argc_; }

  char arg_buffer_[kArgBufferSize];
  char* argv_[kMaxArgCount];
  int argc_ = 0;
  int cursor_ = 0;
  std::string test_path_;
  std::string dbname_;
  Random rnd_;

};


TEST_F(TraceAnalyzerTest, General) {
  GenerateTrace(test_path_ + "/trace");
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "Trace_analyzer test is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE  return RUN_ALL_TESTS();
