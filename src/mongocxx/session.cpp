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

#include <mongocxx/private/session.hh>
#include <mongocxx/session.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/config/private/prelude.hh>
#include <mongocxx/private/client.hh>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

session::session(const mongocxx::client& client, const options::session& options)
    : _impl{stdx::make_unique<impl>(client._get_impl().client_t, options)} {}

const mongocxx::client& session::client() const {
    return *_client;
}

class database session::database(bsoncxx::string::view_or_value name) const& {
    return mongocxx::database{*this, std::move(name)};
}

class database session::operator[](bsoncxx::string::view_or_value name) const& {
    return database(name);
}

session::impl& session::_get_impl() {
    return *_impl;
}

const session::impl& session::_get_impl() const {
    return *_impl;
}

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx