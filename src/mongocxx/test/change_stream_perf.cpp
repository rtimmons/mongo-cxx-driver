
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


    using namespace bsoncxx::builder::basic;
    using namespace std::chrono;
    using namespace std;

    using time_point = std::chrono::time_point<std::chrono::system_clock>;

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

    auto time = [](const string& name, auto f) {
        auto out = make_unique<timed_result>();
        finally _ {*out};
        int result = f();
        out->result = result;
        return out;
    };

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

    void watch_until(const mongocxx::client& client,
                     const time_point end) {
        auto collection = client["db"]["coll"];
        // cleanup before we do anything
        collection.drop();
        collection.insert_one(make_document(kvp("dummy","1")));

        mongocxx::change_stream stream = collection.watch();

        int i = 0;
        int count = 0;
        microseconds total;
        while (system_clock::now() < end) {
            auto insert = time("insert", [&i, &collection]() {
                bsoncxx::document::view_or_value doc = make_document(kvp("a",++i));
                collection.insert_one(doc);
                return 1;
            });
            auto retrieve = time("retrieve", [&stream,&count]() {
                auto it = stream.begin();
                if (count > 0) {
                    it++;
                }
                *it;
                return 0;
            });

            // TODO: make method for this?
            auto op = duration_cast<microseconds>(retrieve->finish.value() - insert->start);
//            cout << "op=" << op.count() << endl;
            total += op;
            ++count;
        }

        std::cout << count << "/" << total.count() << " ops/microseconds" << endl;
        auto ratio = duration_cast<seconds>(microseconds(1) * total.count());
        std::cout << (double(count) / double(ratio.count())) << " ops/seconds" << endl;
    }

}  // namespace


TEST_CASE("Main") {
    mongocxx::instance::current();
    mongocxx::uri uri{"mongodb://localhost:27017/?minPoolSize=3&maxPoolSize=3"};
    mongocxx::pool pool{uri};

    try {
        auto entry = pool.acquire();

        // End in 1 seconds:
        const auto end = std::chrono::system_clock::now() + std::chrono::seconds{10};

        watch_until(*entry, end);

        return;
    } catch (const std::exception& exception) {
        std::cerr << "Caught exception \"" << exception.what() << "\"" << std::endl;
    } catch (...) {
        std::cerr << "Caught unknown exception type" << std::endl;
    }
}
