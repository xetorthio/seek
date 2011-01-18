# seek

seek is a small search engine for java based on [Redis](http://github.com/antirez/redis "Redis").

it uses [Jedis](http://github.com/xetorthio/jedis "Jedis") as a client for Redis

## How do I configure seek?
    List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
    shards.add(new JedisShardInfo("localhost"));
    Config config = new Config();
    Seek.configure(config, shards);

## How do I index my data?

    Seek seek = new Seek();
    Index index = seek.index("items");
    Entry entry = index.add(id);
    entry.addField("category_id", "MLA31594");
    entry.addField("seller_id", "84689862");
    entry.addTag("buy_it_now");
    entry.addText("title",
            "Apple Ipod Classic 160gb 160 8° Generation 40.000 Songs!");
    entry.addOrder("start_date", 1287278020);
    entry.shardBy("seller_id");
    entry.save();    

## How do I search?

    Seek seek = new Seek();
    Search search = seek.search("items", "start_date", new ShardField("seller_id", "84689862"));
    search.field("category_id", "MLA31594", "MLA39056");
    search.tag("buy_it_now", "promotion");
    search.text("title", "ipod 160");
    Result result = search.run(cache, start, end, Search.Order.DESC);

## Can I get general Facets for the shard?

    Seek seek = new Seek();
    Info info = seek.getInfo("items", new ShardField("seller_id", "84689862"));

## Downloads

You can download the latests build at: 
    http://github.com/xetorthio/seek/downloads

For more usage examples check the tests.

## I want to contribute!

That is great! Just fork the project in github. Create a topic branch, write some tests and the feature that you wish to contribute.

To run the tests:

- Use the latest redis master branch.

- Run a instance of redis

- mvn test

Thanks for helping!

## License

Copyright (c) 2011 Jonathan Leibiusky

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

