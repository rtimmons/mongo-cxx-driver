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

#include <cstdlib>
#include <iostream>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

#include <get_server_version.h>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};

    // Change streams require 3.6
    if (get_server_version(conn) < "3.6") {
        return 0;
    }

    auto coll = conn["test"]["coll"];
    coll.drop();

    // Dummy insert to create the database and collection.
    coll.insert_one(make_document(kvp("dummy","document")));

    {
        // Iterate over empty change-stream (no events):
        mongocxx::change_stream stream = coll.watch();
        for (const auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
    }

    {
        // Iterate over a stream with events:
        mongocxx::change_stream stream = coll.watch();
        coll.insert_one(make_document(kvp("some", "event")));
        for (const auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
    }

    {
        // A "forever" loop that continuously processes events until exceptions are encountered.
        // Batch 100 events at a time and wait up to 1 second before polling again.

        // Configure appropriate values for your application:
        const std::int32_t batch_size {100};
        const std::chrono::milliseconds await_time {1000};
        const mongocxx::stdx::optional<long> max_iterations = 1; // Use an empty optional for no max

        mongocxx::options::change_stream options;
        options.batch_size(batch_size);
        options.max_await_time(await_time);

        mongocxx::change_stream stream = coll.watch(options);
        long iteration = 0;
        while(!max_iterations || iteration++ < max_iterations) {
            // Server errors propagate as exceptions and will cause our loop to exit.
            for(const auto& event : stream) {
                std::cout << bsoncxx::to_json(event) << std::endl;
            }
        }
    }
}