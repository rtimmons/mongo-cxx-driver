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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

std::string get_server_version(const mongocxx::client& client) {
    bsoncxx::builder::basic::document server_status{};
    server_status.append(bsoncxx::builder::basic::kvp("serverStatus", 1));
    bsoncxx::document::value output = client["test"].run_command(server_status.extract());

    return bsoncxx::string::to_string(output.view()["version"].get_utf8().value);
}

void watch_forever(mongocxx::pool& pool) {
    auto client = pool.acquire();

    mongocxx::options::change_stream options;
    // Wait up to 1 second before polling again.
    const std::chrono::milliseconds await_time{1000};
    options.max_await_time(await_time);

    auto collection = (*client)["db"]["coll"];
    mongocxx::change_stream stream = collection.watch(options);

    while (true) {
        for (const auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
    }
}

int main() {
    mongocxx::instance inst{};
    mongocxx::uri uri{"mongodb://localhost:27017/?minPoolSize=3&maxPoolSize=3"};
    mongocxx::pool pool{uri};

    try {
        {
            auto client = pool.acquire();
            if (get_server_version(*client) < "3.6") {
                return 1;
            }
        }

        watch_forever(pool);

        return 0;
    } catch (...) {
        auto e = std::current_exception();
        try {
            if (e) {
                std::rethrow_exception(e);
            }
        } catch (const std::exception& e) {
            std::cerr << "Caught exception \"" << e.what() << "\"" << std::endl;
        }
    }

    return 1;
}
