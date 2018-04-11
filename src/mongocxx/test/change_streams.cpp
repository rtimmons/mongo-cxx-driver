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

#include <atomic>
#include <chrono>
#include <iostream>
#include <list>
#include <queue>
#include <thread>
#include <vector>

#include <bson.h>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test_util/catch.hh>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/exception/write_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pipeline.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/test_util/client_helpers.hh>
#include <mongocxx/write_concern.hpp>

#include <third_party/catch/include/helpers.hpp>

namespace {

using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

using namespace mongocxx;

///
/// Create a single-item document
/// E.g. doc("foo", 123) creates {"foo":123}
///
template <typename T>
bsoncxx::document::value doc(std::string key, T val) {
    bsoncxx::builder::basic::document out{};
    out.append(kvp(key, val));
    return std::move(out.extract());
}

enum class response_type {
    doc,
    empty,
    error,
};

class response {
   public:
    using ptr = std::unique_ptr<bson_t, void (*)(bson_t*)>;

    static ptr as_doc(bsoncxx::document::view_or_value doc) {
        return std::move(
            ptr{bson_new_from_data(doc.view().data(), doc.view().length()), bson_destroy});
    }

    response(response_type r, bsoncxx::document::view_or_value&& doc)
        : resp_{r}, doc_{std::move(as_doc(std::move(doc)))} {}

    response(response&& other) : resp_{other.resp_}, doc_{std::move(other.doc_)} {}

    response(const response& other) = delete;

    bool error() const {
        return resp_ == response_type::error;
    }

    bool next() const {
        return resp_ == response_type::doc;
    }

    bool empty() const {
        return resp_ == response_type::empty;
    }

    bson_t* bson() const {
        return doc_.get();
    }

   private:
    const response_type resp_;
    ptr doc_;
};

// This mock is a bit more complicated than it wants to be,
// but it tries to be as close to the C driver's actual behavior
// as possible.
struct mock_stream_state {
    std::vector<response> responses;
    unsigned long position;

    bool destroyed = false;
    int watches = 0;

    mock_stream_state() : position{0}, responses{} {}

    template <typename... Args>
    mock_stream_state& then(Args&&... args) {
        responses.emplace_back(std::forward<Args>(args)...);
        return *this;
    }

    ~mock_stream_state() {
        REQUIRE(destroyed);
    }

    bool next(mongoc_change_stream_t* stream, const bson_t** bson) {
        auto& curr = current();
        if (curr.next()) {
            *bson = curr.bson();
            ++position;
            return true;
        }
        return curr.next();
    }

    bool error(const mongoc_change_stream_t* stream, bson_error_t* err, const bson_t** bson) {
        auto& curr = current();
        if (curr.error()) {
            *bson = curr.bson();
            bson_set_error(err,
                           MONGOC_ERROR_CURSOR,
                           MONGOC_ERROR_CHANGE_STREAM_NO_RESUME_TOKEN,
                           "expected error");
            return true;
        }
        if (curr.empty()) {
            ++position;
        }

        return curr.error();
    }

    const response& current() {
        // If fail here, not enough mocked state (i.e. error in the test setup).
        REQUIRE(position < responses.size());
        return responses.at(position);
    }

    void destroy(mongoc_change_stream_t* stream) {
        destroyed = true;
    }

    mongoc_change_stream_t* watch(const mongoc_collection_t* coll,
                                  const bson_t* pipeline,
                                  const bson_t* opts) {
        ++watches;
        return nullptr;
    }

    //
    // The _op methods below hook into c function mocks to call methods
    // on this instance instead.
    //

    template <typename F>
    void next_op(F&& f) {
        f->interpose([&](mongoc_change_stream_t* stream, const bson_t** bson) -> bool {
             return this->next(stream, bson);
         }).forever();
    }

    template <typename F>
    void watch_op(F&& f) {
        f->interpose([&](const mongoc_collection_t* coll,
                         const bson_t* pipeline,
                         const bson_t* opts) -> mongoc_change_stream_t* {
             return this->watch(coll, pipeline, opts);
         }).forever();
    }

    template <typename F>
    void destroy_op(F&& f) {
        f->interpose([&](mongoc_change_stream_t* stream) -> void {
             return this->destroy(stream);
         }).forever();
    }

    template <typename F>
    void error_op(F&& f) {
        f->interpose([&](const mongoc_change_stream_t* stream,
                         bson_error_t* err,
                         const bson_t** bson) -> bool {
             return this->error(stream, err, bson);
         }).forever();
    }
};

SCENARIO("Mock streams and error-handling") {
    MOCK_CHANGE_STREAM

    instance::current();
    client mongodb_client{uri{}};
    options::change_stream options{};

    database db = mongodb_client["streams"];
    collection events = db["events"];

    using namespace std;

    mock_stream_state state;
    // Hook into c-lib mock functions.
    // This can be done before the mock's script is established.
    state.watch_op(collection_watch);
    state.destroy_op(change_stream_destroy);
    state.next_op(change_stream_next);
    state.error_op(change_stream_error_document);

    WHEN("We have no errors and no mock events") {
        state.then(response_type::empty, make_document());
        state.then(response_type::empty, make_document());
        state.then(response_type::empty, make_document());
        state.then(response_type::empty, make_document());

        THEN("The distance is zero repeatedly") {
            // This is more of a test of the test / mock infrastructure but it's useful when
            // changing that!
            auto stream = events.watch();
            REQUIRE(distance(stream.begin(), stream.end()) == 0);
            REQUIRE(distance(stream.begin(), stream.end()) == 0);

            auto stream2 = events.watch();
            REQUIRE(distance(stream2.begin(), stream.end()) == 0);
            REQUIRE(distance(stream2.begin(), stream.end()) == 0);
        }
    }

    WHEN("We have one message") {
        state.then(response_type::doc, make_document(kvp("a", "b")));
        state.then(response_type::empty, make_document());

        THEN("The distance is one") {
            auto stream = events.watch();
            REQUIRE(distance(stream.begin(), stream.end()) == 1);
        }
    }

    WHEN("We have an error as the first interaction") {
        state.then(response_type::error, make_document());
        auto stream = events.watch();
        THEN("We throw an exception when .begin()ing") {
            REQUIRE_THROWS(stream.begin());
            THEN("We repeatedly have zero items") {
                // Don't require more mock steps because we reach a dead state internally
                // and don't reach out to the C driver after we encounter an error.
                REQUIRE(distance(stream.begin(), stream.end()) == 0);
                REQUIRE(distance(stream.begin(), stream.end()) == 0);
                REQUIRE(distance(stream.begin(), stream.end()) == 0);

                // We also segfault on this, but it's undefined behavior that
                // we are allowed to change in the future.
                //     auto it = stream.begin();
                //     *it;
            }
        }
    }

    WHEN("We have one event and then an error") {
        state.then(response_type::doc, make_document(kvp("some", "event")));
        state.then(response_type::error, make_document());
        auto stream = events.watch();

        THEN("We can access a single event") {
            auto it = stream.begin();
            REQUIRE(*it == make_document(kvp("some", "event")).view());

            THEN("We're not at end") {
                REQUIRE(it != stream.end());
            }

            THEN("We throw on subsequent increment") {
                REQUIRE_THROWS(it++);
                REQUIRE(it == stream.end());

                // Debatable if we want to require this behavior since it's
                // inconsistent with other cases of dereferencing something
                // that's == end(). Important thing is that we don't maintain
                // a handle on the previous event.
                REQUIRE(*it == make_document().view());
                REQUIRE(*it == make_document().view());
            }
        }
    }
}

SCENARIO("A non-existent collection is watched") {
    instance::current();
    client mongodb_client{uri{}};
    options::change_stream options{};

    database db = mongodb_client["does_not_exist"];
    collection events = db["does_not_exist"];
    GIVEN("We try to watch it") {
        THEN("We get an error") {
            change_stream stream = events.watch();
            REQUIRE_THROWS(stream.begin());
        }
    }
}

SCENARIO("A collection is watched") {
    instance::current();
    client mongodb_client{uri{}};
    options::change_stream options{};

    database db = mongodb_client["streams"];
    collection events = db["events"];

    THEN("We can copy- and move-assign iterators") {
        auto x = events.watch();
        REQUIRE(events.insert_one(doc("a", "b")));

        auto one = x.begin();
        REQUIRE(one != x.end());

        auto two = one;
        REQUIRE(two != x.end());

        REQUIRE(one == two);
        REQUIRE(two == one);

        // move-assign (although it's trivially-copiable)
        auto three = std::move(two);

        REQUIRE(three != x.end());
        REQUIRE(one == three);

        // two is in moved-from state. Technically `three == two` but that's not required.
    }

    GIVEN("We have a default change stream and no events") {
        THEN("We can move-assign it") {
            change_stream stream = events.watch();
            change_stream move_copy = std::move(stream);
        }
        THEN("We can move-construct it") {
            change_stream stream = events.watch();
            change_stream move_constructed = change_stream{std::move(stream)};
        }
        THEN(".end == .end") {
            change_stream x = events.watch();
            REQUIRE(x.end() == x.end());

            auto e = x.end();
            REQUIRE(e == e);
        }
        THEN("We don't have any events") {
            change_stream x = events.watch();
            REQUIRE(x.begin() == x.end());

            // a bit pedantic
            auto b = x.begin();
            REQUIRE(b == b);
            auto e = x.end();
            REQUIRE(e == e);

            REQUIRE(e == b);
            REQUIRE(b == e);
        }
        THEN("Empty iterator is equivalent to user-constructed iterator") {
            change_stream x = events.watch();
            REQUIRE(x.begin() == change_stream::iterator{});
            REQUIRE(x.end() == change_stream::iterator{});
        }
    }

    GIVEN("We have a single event") {
        change_stream x = events.watch();
        REQUIRE(events.insert_one(doc("a", "b")));

        THEN("We can receive an event") {
            auto it = *(x.begin());
            REQUIRE(it["fullDocument"]["a"].get_utf8().value == stdx::string_view("b"));
        }

        THEN("iterator equals itself") {
            auto it = x.begin();
            REQUIRE(it == it);

            auto e = x.end();
            REQUIRE(e == e);

            REQUIRE(it != e);
            REQUIRE(e != it);
        }

        THEN("We can deref iterator with value multiple times") {
            auto it = x.begin();
            auto a = *it;
            auto b = *it;
            REQUIRE(a["fullDocument"]["a"].get_utf8().value == stdx::string_view("b"));
            REQUIRE(b["fullDocument"]["a"].get_utf8().value == stdx::string_view("b"));
        }

        THEN("Calling .begin multiple times doesn't advance state") {
            auto a = *(x.begin());
            auto b = *(x.begin());
            REQUIRE(a == b);
        }

        THEN("We have no more events after the first one") {
            auto it = x.begin();
            it++;
            REQUIRE(it == x.end());
            REQUIRE(x.begin() == x.end());
        }

        THEN("Past end is empty document") {
            auto it = x.begin();
            it++;
            REQUIRE(*it == bsoncxx::builder::basic::document{});
        }

        THEN("Can dereference end()") {
            auto it = x.begin();
            it++;
            REQUIRE(*it == *it);
        }
    }

    GIVEN("We have multiple events") {
        change_stream x = events.watch();

        REQUIRE(events.insert_one(doc("a", "b")));
        REQUIRE(events.insert_one(doc("c", "d")));

        THEN("A range-based for loop iterates twice") {
            int count = 0;
            for (const auto& v : x) {
                ++count;
            }
            REQUIRE(count == 2);
        }

        THEN("distance is two") {
            auto dist = std::distance(x.begin(), x.end());
            REQUIRE(dist == 2);
        }

        THEN("We can advance two iterators through the events") {
            auto one = x.begin();
            auto two = x.begin();

            REQUIRE(one != x.end());
            REQUIRE(two != x.end());

            one++;

            REQUIRE(one != x.end());
            REQUIRE(two != x.end());

            two++;

            REQUIRE(one == x.end());
            REQUIRE(two == x.end());
        }
    }

    GIVEN("We have already advanced past the first set of events") {
        change_stream x = events.watch();

        REQUIRE(events.insert_one(doc("a", "b")));
        REQUIRE(events.insert_one(doc("c", "d")));

        REQUIRE(std::distance(x.begin(), x.end()) == 2);

        WHEN("We try to look for more events") {
            REQUIRE(x.begin() == x.end());
        }

        WHEN("There are more events we can find them") {
            REQUIRE(events.insert_one(doc("e", "f")));
            REQUIRE(std::distance(x.begin(), x.end()) == 1);
        }
    }

    // Reset state. This should stay at the end of this SCENARIO block.
    events.drop();
}

}  // namepsace
