// Copyright 2017 MongoDB Inc.
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

#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/session.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class session::impl {
   public:
    impl() : session_t(nullptr) {}

    impl(const class mongocxx::client* client, options::session opts)
        : client{client}, session_t(nullptr) {
        std::unique_ptr<mongoc_session_opt_t, decltype(libmongoc::session_opts_destroy)>
            session_opts{libmongoc::session_opts_new(), libmongoc::session_opts_destroy};

        if (opts.write_concern()) {
            // waiting on c driver
        }

        if (opts.read_preference()) {
            // waiting on c driver
        }

        if (opts.read_concern()) {
            // waiting on c driver
        }

        session_t =
            libmongoc::client_start_session(client->_get_impl().client_t, session_opts.get(), NULL);
    }

    ~impl() {
        libmongoc::session_destroy(session_t);
    }

    const class mongocxx::client* client;
    mongoc_session_t* session_t;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>