
#include <chrono>
#include <iostream>
#include <optional>

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

    template<typename F, typename O>
    class timed {
    private:
        const F lambda;
        std::optional<time_point> start;
        std::optional<time_point> end;
        struct finally {
            timed& t;
            explicit finally(timed& t_) : t{t_} {}
            ~finally() {
                t.end = system_clock::now();
            }
        };
    public:
        timed(F lambda_)
        : lambda{std::move(lambda_)} {}

        std::optional<dur> time() {
            if (end) {
                return end.value() - start.value();
            }
            return std::make_optional<dur>(nullptr);
        }
        O operator()() {
            start = system_clock::now();
            finally{*this};
            return lambda();
        }
    };

    template<typename F, typename O>
    ostream& operator<<(ostream& out, const timed<F,O>& timed) {
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
            const function<void(void)> f = [&](){
                bsoncxx::document::view_or_value doc = make_document(kvp("a",++i));
                collection.insert_one(doc);
            };
            timed<decltype(f), void> t (f);
            t();
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

        // End in 10 seconds:
        const auto end = std::chrono::system_clock::now() + std::chrono::seconds{10};

        watch_until(*entry, end);

        return;
    } catch (const std::exception& exception) {
        std::cerr << "Caught exception \"" << exception.what() << "\"" << std::endl;
    } catch (...) {
        std::cerr << "Caught unknown exception type" << std::endl;
    }
}
