
#include <chrono>
#include <iostream>
#include <optional>
#include <ctime>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

#include <third_party/catch/include/helpers.hpp>
#include <third_party/catch/include/catch.hpp>

namespace {

    using namespace std::chrono;
    using namespace std;
    using time_point = std::chrono::time_point<std::chrono::system_clock>;
    using dur = std::chrono::duration<long>;

    struct timed_result {
        const time_point start;
        optional<time_point> finish;
        optional<int> result;

        explicit timed_result()
        : start{system_clock::now()},
          finish{make_optional<time_point>()},
          result{make_optional<int>()} {}
    };

    struct finally {
        explicit finally(timed_result& result_) : result{result_} {}

        void operator=(const finally&) = delete;
        finally(finally&&other) = delete;

        timed_result& result;
        ~finally() {
            result.finish = system_clock::now();
        }
    };
    auto time = [](string name, auto f) {
        auto out = make_unique<timed_result>();
        finally q {*out};
        int result = f();
        out->result = result;
        return out;
    };

//    template<typename F, typename O>
//    ostream& operator<<(ostream& out, const timed<F,O>& timed) {
//        return out;
//    }

    ostream& operator<<(ostream& out, const time_point& p) {
        std::time_t t = system_clock::to_time_t(p);
        out << std::ctime(&t);
        return out;
    }

    ostream& operator<<(ostream& out, const optional<time_point>& p) {
        if(p) {
            out << p.value();
        } else {
            out << "(?)";
        }
        return out;
    }

    template<class T>
    class TD;
    using namespace bsoncxx::builder::basic;

    void watch_until(const mongocxx::client& client,
                     const time_point end) {
        auto collection = client["db"]["coll"];
        mongocxx::change_stream stream = collection.watch();

        auto i = 0;

        while (system_clock::now() < end) {
            auto result = time("insert", [&](){
                bsoncxx::document::view_or_value doc = make_document(kvp("a",++i));
                collection.insert_one(doc);
                return true;
            });

            std::cout << result->start << std::endl;
            std::cout << result->result.value() << std::endl;
            std::cout << result->finish << std::endl;
            for (const auto& event : stream) {
                std::cout << bsoncxx::to_json(event) << std::endl;
            }
        }
    }

}  // namespace


TEST_CASE("Main") {
    mongocxx::instance::current();
    mongocxx::uri uri{"mongodb://localhost:27017/?minPoolSize=3&maxPoolSize=3"};
    mongocxx::pool pool{uri};

    try {
        auto entry = pool.acquire();

        // End in 1 seconds:
        const auto end = std::chrono::system_clock::now() + std::chrono::seconds{1};

        watch_until(*entry, end);

        return;
    } catch (const std::exception& exception) {
        std::cerr << "Caught exception \"" << exception.what() << "\"" << std::endl;
    } catch (...) {
        std::cerr << "Caught unknown exception type" << std::endl;
    }
}
