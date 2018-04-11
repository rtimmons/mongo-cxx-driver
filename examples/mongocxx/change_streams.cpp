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
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

std::string get_server_version(const mongocxx::client& client) {
    bsoncxx::builder::basic::document server_status{};
    server_status.append(bsoncxx::builder::basic::kvp("serverStatus", 1));
    bsoncxx::document::value output = client["test"].run_command(server_status.extract());

    return bsoncxx::string::to_string(output.view()["version"].get_utf8().value);
}

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};
    auto coll = conn["test"]["coll"];
    coll.drop();

    // Change streams require 3.6
    if (get_server_version(conn) < "3.6") {
        return 0;
    }

    {
        // Iterate over empty change-stream (no events):
        mongocxx::change_stream stream = coll.watch();
        for (auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
    }

    {
        // Iterate over a stream with events:
        mongocxx::change_stream stream = coll.watch();
        coll.insert_one(make_document(kvp("some", "event")));
        for (auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
    }
}