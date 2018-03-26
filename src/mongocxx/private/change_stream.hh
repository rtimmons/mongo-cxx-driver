// Copyright 2018-present MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class change_stream::impl {
   public:
    // lifecycle of the cursor
    // k_started means that libmongoc::change_stream_next has been called at least once.
    // k_pending means it hasn't
    // k_dead means that an error was indicated by a call to next
    // TODO: how to handle error cases?
    enum class state { k_pending = 0, k_started = 1, k_dead = 2 };

    // TODO: should we support change_stream being null? not sure what that would indicate
    impl(mongoc_change_stream_t* change_stream)
        : change_stream_t(change_stream),
          status{change_stream ? state::k_pending : state::k_dead},
          exhausted(!change_stream) {}

    ~impl() {
        // TODO: do we leak this->doc here?
        libmongoc::change_stream_destroy(change_stream_t);
    }

    bool has_started() const {
        return status >= state::k_started;
    }

    bool is_dead() const {
        return status == state::k_dead;
    }

    bool is_exhausted() const {
        return exhausted;
    }

    void mark_dead() {
        mark_nothing_left();
        status = state::k_dead;
    }

    void mark_nothing_left() {
        doc = bsoncxx::document::view{};
        exhausted = true;
        status = state::k_pending;
    }

    void mark_started() {
        status = state::k_started;
        exhausted = false;
    }

    void advance_iterator() {
        bson_error_t error{};

        const bson_t* out;
        if (libmongoc::change_stream_next(this->change_stream_t, &out)) {
            this->doc = bsoncxx::document::view{bson_get_data(out), out->len};
        } else if (libmongoc::change_stream_error_document(this->change_stream_t, &error, &out)) {
            // TODO: better error-handling?
            // TODO: do we care about modifying out in error scenarios?
            this->mark_dead();
            // TODO: test case of this - that after error we don't hold onto last doc
            // TODO: test accessing the documenting with operator* and operator-> after an error shouldn't crash.
            this->doc = bsoncxx::document::view{};
            throw_exception<query_exception>(error);
        } else {
            this->mark_nothing_left();
        }
    }

    mongoc_change_stream_t* change_stream_t;
    bsoncxx::document::view doc;
    state status;
    bool exhausted;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
