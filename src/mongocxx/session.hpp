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

#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/string/view_or_value.hpp>
#include <mongocxx/options/session.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class MONGOCXX_API session {
   public:
    ///
    /// Creates a new session.
    ///
    /// @param client
    ///   The client that created this session.
    /// @param options
    ///   Additional options for configuring the session.
    ///
    session(const mongocxx::client& client, const options::session& options = {});

    session(const session&) = delete;

    session& operator=(const session&) = delete;

    ///
    /// Move constructs a session.
    ///
    session(session&&) noexcept = default;

    ///
    /// Move assigns a session.
    ///
    session& operator=(session&&) noexcept = default;

    ///
    /// Ends and destroys the session.
    ///
    ~session() = default;

    ///
    /// Gets the client that started this session.
    ///
    /// @return
    ///   The client that started this session.
    ///
    const mongocxx::client& client() const;

    ///
    /// Gets the current write concern for this session.
    ///
    /// @return
    ///   The current @c write_concern.
    ///
    class write_concern write_concern() const;

    ///
    /// Gets the current read concern for this session.
    ///
    /// @return
    ///   The current @c read_concern.
    ///
    class read_concern read_concern() const;

    ///
    /// Returns the current read preference for this session.
    ///
    /// @return
    ///   The current @c read_preference.
    ///
    /// @see
    ///   https://docs.mongodb.com/master/core/read-preference/
    ///
    class read_preference read_preference() const;

    ///
    /// Returns whether the driver session has ended. A driver session has ended when endSession has
    /// been called.
    ///
    /// @return
    ///   whether the driver session as ended.
    ///
    bool has_ended() const;

    ///
    /// Obtains a database that represents a logical grouping of collections on a MongoDB server.
    ///
    /// @note
    ///   A database cannot be obtained from a temporary session object.
    ///
    /// @param name
    ///   The name of the database to get.
    ///
    /// @return
    ///   The database.
    ///
    class database database(bsoncxx::string::view_or_value name) const&;
    class database database(bsoncxx::string::view_or_value name) const&& = delete;

    ///
    /// Allows the syntax @c session["db_name"] as a convenient shorthand for the
    /// session::database() method by implementing the array subscript operator.
    ///
    /// @note
    ///   A database cannot be obtained from a temporary session object.
    ///
    /// @param name
    ///   The name of the database.
    ///
    /// @return
    ///   Client side representation of a server side database.
    ///
    class database operator[](bsoncxx::string::view_or_value name) const&;
    class database operator[](bsoncxx::string::view_or_value name) const&& = delete;

   private:
    friend class database;

    MONGOCXX_PRIVATE session() = default;

    class MONGOCXX_PRIVATE impl;
    class client;

    MONGOCXX_PRIVATE impl& _get_impl();
    MONGOCXX_PRIVATE const impl& _get_impl() const;

    std::unique_ptr<impl> _impl;
    mongocxx::client* _client;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
