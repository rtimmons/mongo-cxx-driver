// Copyright 2014 MongoDB Inc.
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

#include <mongocxx/client.hpp>

#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/private/error_category.hh>
#include <mongocxx/exception/private/mongoc_error.hh>
#include <mongocxx/options/private/ssl.hh>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/read_concern.hh>
#include <mongocxx/private/read_preference.hh>
#include <mongocxx/private/session.hh>
#include <mongocxx/private/uri.hh>
#include <mongocxx/private/write_concern.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

client::client() noexcept = default;

client::client(const class uri& uri, const options::client& options) {
#if defined(MONGOCXX_ENABLE_SSL) && defined(MONGOC_ENABLE_SSL)
    if (options.ssl_opts()) {
        if (!uri.ssl())
            throw exception{error_code::k_invalid_parameter,
                            "cannot set SSL options if 'ssl=true' not in URI"};

        auto mongoc_opts = options::make_ssl_opts(*options.ssl_opts());
        _impl->ssl_options = std::move(mongoc_opts.second);
        libmongoc::client_set_ssl_opts(_get_impl().client_t, &mongoc_opts.first);
    }
#else
    if (uri.ssl() || options.ssl_opts())
        throw exception{error_code::k_ssl_not_supported};
#endif
    auto new_client = libmongoc::client_new_from_uri(uri._impl->uri_t);
    if (!new_client) {
        // Shouldn't happen after checks above, but future libmongoc's may change behavior.
        throw exception{error_code::k_invalid_parameter, "could not construct client from URI"};
    }

    _impl = stdx::make_unique<impl>(std::move(new_client));
}

client::client(void* implementation)
    : _impl{stdx::make_unique<impl>(static_cast<::mongoc_client_t*>(implementation))} {}

client::client(client&&) noexcept = default;
client& client::operator=(client&&) noexcept = default;

client::~client() = default;

client::operator bool() const noexcept {
    return static_cast<bool>(_impl);
}

void client::read_concern_deprecated(class read_concern rc) {
    auto client_t = _get_impl().client_t;
    libmongoc::client_set_read_concern(client_t, rc._impl->read_concern_t);
}

void client::read_concern(class read_concern rc) {
    return read_concern_deprecated(std::move(rc));
}

class read_concern client::read_concern() const {
    auto rc = libmongoc::client_get_read_concern(_get_impl().client_t);
    return {stdx::make_unique<read_concern::impl>(libmongoc::read_concern_copy(rc))};
}

void client::read_preference_deprecated(class read_preference rp) {
    libmongoc::client_set_read_prefs(_get_impl().client_t, rp._impl->read_preference_t);
}

void client::read_preference(class read_preference rp) {
    return read_preference_deprecated(std::move(rp));
}

class read_preference client::read_preference() const {
    class read_preference rp(stdx::make_unique<read_preference::impl>(
        libmongoc::read_prefs_copy(libmongoc::client_get_read_prefs(_get_impl().client_t))));
    return rp;
}

class uri client::uri() const {
    class uri connection_string(stdx::make_unique<uri::impl>(
        libmongoc::uri_copy(libmongoc::client_get_uri(_get_impl().client_t))));
    return connection_string;
}

void client::write_concern_deprecated(class write_concern wc) {
    libmongoc::client_set_write_concern(_get_impl().client_t, wc._impl->write_concern_t);
}

void client::write_concern(class write_concern wc) {
    return write_concern_deprecated(std::move(wc));
}

class write_concern client::write_concern() const {
    class write_concern wc(stdx::make_unique<write_concern::impl>(
        libmongoc::write_concern_copy(libmongoc::client_get_write_concern(_get_impl().client_t))));
    return wc;
}

class database client::database(bsoncxx::string::view_or_value name) const& {
    return mongocxx::database(*this, std::move(name));
}

cursor client::list_databases() const {
    return libmongoc::client_find_databases_with_opts(_get_impl().client_t, nullptr);
}

class session client::start_session(const mongocxx::options::session& options) {
    return session(this, options);
}

const client::impl& client::_get_impl() const {
    if (!_impl) {
        throw logic_error{error_code::k_invalid_client_object};
    }
    return *_impl;
}

client::impl& client::_get_impl() {
    auto cthis = const_cast<const client*>(this);
    return const_cast<client::impl&>(cthis->_get_impl());
}

#ifdef MONGOCXX_TESTING
void* client_t_from_client(client& client) {
    return client._get_impl().client_t;
}
#endif

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx
