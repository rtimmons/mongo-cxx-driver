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

#include <iostream>

#include <bson.h>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/test_util/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pipeline.hpp>

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

///
/// Generates lambda/interpose for change_stream_next
///
auto gen_next = [](bool has_next) {
    return [=](mongoc_change_stream_t* stream, const bson_t** bson) mutable -> bool {
        *bson = BCON_NEW("some", "doc");
        return has_next;
    };
};

///
/// Generates lambda/interpose for change_stream_error_document
///
auto gen_error = [](bool has_error) {
    return [=](const mongoc_change_stream_t* stream,
               bson_error_t* err,
               const bson_t** bson) -> bool {
        if (has_error) {
            bson_set_error(err, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CHANGE_STREAM_NO_RESUME_TOKEN, "expected error");
            *bson = BCON_NEW("from", "gen_error"); // different from what's in gen_next
        }
        return has_error;
    };
};


auto watch_interpose = [](const mongoc_collection_t* coll,
                          const bson_t* pipeline,
                          const bson_t* opts) -> mongoc_change_stream_t* {
    return nullptr;
};

auto destroy_interpose = [](mongoc_change_stream_t* stream) -> void {};


SCENARIO("Mock streams and error-handling") {
    MOCK_CHANGE_STREAM

    instance::current();
    client mongodb_client{uri{}};
    options::change_stream options{};

    database db = mongodb_client["streams"];
    collection events = db["events"];

    // nop watch and destroy
    collection_watch->interpose(watch_interpose).forever();
    change_stream_destroy->interpose(destroy_interpose).forever();

    WHEN("We have one event and then an error") {
        auto stream = events.watch();
        change_stream_next->interpose(gen_next(true));

        THEN("We can access a single event.") {
            auto it = stream.begin();
            REQUIRE(*it == make_document(kvp("some", "doc")).view());

            // next call indicates no next and no error
            change_stream_next->interpose(gen_next(false));
            change_stream_error_document->interpose(gen_error(true));

            THEN("We throw on subsequent increment") {
                REQUIRE_THROWS(it++);

                THEN("We're at the end") {
                    REQUIRE(it == stream.end());
                }
                THEN("We remain at error state") {
                    REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
                    REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
                }
                THEN("We don't hold on to previous document") {
                    // Debatable if we want to require this behavior since it's
                    // inconsistent with other cases of dereferencing something
                    // that's == end(). Important thing is that we don't maintain
                    // a handle on the previous event.
                    REQUIRE(*it == make_document().view());
                }
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

TEST_CASE("We give an invalid pipeline") {
    instance::current();
    client mongodb_client{uri{}};

    database db = mongodb_client["streams"];
    collection events = db["events"];

    pipeline p;
    p.match(make_document(kvp("$foo",-1)));

    SECTION("An error is thrown on .begin() even if no events") {
        auto stream = events.watch(p);
        REQUIRE_THROWS(stream.begin());
    }
    SECTION("After error, begin == end repeatedly") {
        auto stream = events.watch(p);
        REQUIRE_THROWS(stream.begin());
        REQUIRE(stream.begin() == stream.end());
        REQUIRE(stream.begin() == stream.end());
        REQUIRE(stream.end() == stream.begin());
    }
    SECTION("No error on .end") {
        auto stream = events.watch(p);
        REQUIRE(stream.end() == stream.end());
    }
}

SCENARIO("Documentation Examples") {
    instance::current();
    client mongodb_client{uri{}};

    database db = mongodb_client["streams"];
    collection inventory = db["events"];

    WHEN("Example 1") {
        change_stream stream = inventory.watch();
        for (auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
    }

    WHEN("Example 1, Version 2") {
        change_stream stream = inventory.watch();
        change_stream::iterator iterator = stream.begin();
        // It is undefined to dereference .begin() when .begin() == .end()
        if (iterator != stream.end()) {
            bsoncxx::document::view event = *iterator;
        }
    }

    WHEN("Example 2") {
        options::change_stream options;
        options.full_document(bsoncxx::string::view_or_value{"updateLookup"});
        change_stream stream = inventory.watch(options);
        for (auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
    }

    WHEN("Example 3") {
        stdx::optional<bsoncxx::document::view_or_value> resume_token;
        change_stream stream = inventory.watch();
        for (auto& event : stream) {
            resume_token = bsoncxx::document::view_or_value{event["_id"].get_document()};
        }

        if (resume_token) {
            options::change_stream options;
            options.resume_after(resume_token.value());
            change_stream resumed = inventory.watch(options);
            for (auto& event : stream) {
                std::cout << bsoncxx::to_json(event) << std::endl;
            }
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

    GIVEN("We only want to see update operations") {
        // Get full doc and deltas but only for updates
        mongocxx::options::change_stream opts;
        opts.full_document(bsoncxx::string::view_or_value{"updateLookup"});

        mongocxx::pipeline pipeline;
        pipeline.match(make_document(kvp("operationType", "update")));

        mongocxx::change_stream stream = events.watch(pipeline, opts);

        // create a document and then update it
        events.insert_one(make_document(kvp("_id", "one"), kvp("a", "a")));
        events.update_one(make_document(kvp("_id", "one")),
                          make_document(kvp("$set", make_document(kvp("a", "A")))));
        events.delete_one(make_document(kvp("_id", "one")));

        THEN("We only see updates :)") {
            auto n_events = std::distance(stream.begin(), stream.end());
            REQUIRE(n_events == 1);
        }
    }

    // Reset state. This should stay at the end of this SCENARIO block.
    events.drop();
}

}  // namepsace
