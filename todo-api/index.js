const express = require("express");
const bodyParser = require("body-parser");
const { Pool } = require("pg");
const redis = require("redis");
const { Client } = require("@elastic/elasticsearch");
const envProps = require("./env_props");

// Initializing the Express Framework /////////////////////////////////////////////////////
const app = express();
const port = 3000;
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true,
  })
);

// Postgres Client Setup /////////////////////////////////////////////////////
const postgresClient = new Pool({
  host: envProps.postgresHost,
  port: envProps.postgresPort,
  database: envProps.postgresDatabase,
  user: envProps.postgresUser,
  password: envProps.postgresPassword,
});
postgresClient.on("connect", () => console.log("Postgres client connected"));
postgresClient.on("error", (err) =>
  console.log("Something went wrong with Postgres: " + err)
);

postgresClient
  .query(
    "CREATE TABLE IF NOT EXISTS todo (id SERIAL PRIMARY KEY, title TEXT UNIQUE NOT NULL)"
  )
  .catch((err) => console.log(err));

// Redis Client Setup (v4+ compatible) /////////////////////////////////////////////////////
const redisClient = redis.createClient({
  url: `redis://${envProps.redisHost}:${envProps.redisPort}`,
  socket: {
    reconnectStrategy: () => 1000,
  },
});
redisClient.on("connect", () => console.log("Redis client connected"));
redisClient.on("error", (err) =>
  console.log("Something went wrong with Redis: " + err)
);

redisClient.connect().catch(console.error);

// Elasticsearch Client Setup (ES 8.x compatible) ///////////////////////////////////////////////
const elasticClient = new Client({
  node: `http://${envProps.elasticHost}:${envProps.elasticPort}`,
});

const TODO_SEARCH_INDEX_NAME = "todos";

// Check Elasticsearch connection and create index
async function setupElasticsearch() {
  try {
    // Ping to check connection
    await elasticClient.ping();
    console.log("Elasticsearch client connected");

    // Create index if it doesn't exist
    const indexExists = await elasticClient.indices.exists({
      index: TODO_SEARCH_INDEX_NAME,
    });

    if (!indexExists) {
      await elasticClient.indices.create({
        index: TODO_SEARCH_INDEX_NAME,
      });
      console.log("Created a new Elastic index: " + TODO_SEARCH_INDEX_NAME);
    } else {
      console.log("Elastic index already exists: " + TODO_SEARCH_INDEX_NAME);
    }
  } catch (error) {
    console.error("Elasticsearch setup error: " + error);
  }
}

setupElasticsearch();

// Set up the API routes /////////////////////////////////////////////////////

// Get all todos
app.route("/api/v1/todos").get(async (req, res) => {
  console.log("CALLED GET api/v1/todos");
  res.setHeader("Content-Type", "application/json");

  try {
    // First, try get todos from cache (get all members of Set)
    const cachedTodoSet = await redisClient.sMembers("todos");

    if (cachedTodoSet && cachedTodoSet.length > 0) {
      const todos = cachedTodoSet.map((title) => ({ title }));
      console.log("  Got todos from Redis cache: ", todos);
      return res.send(todos);
    }

    // Nothing in cache, get from database
    const todoRows = await postgresClient.query("SELECT title FROM todo");
    console.log("  Got todos from PostgreSQL db: ", todoRows.rows);
    res.send(todoRows.rows);
  } catch (error) {
    console.log("Error fetching todos: " + error);
    res.status(500).send({ error: "Failed to fetch todos" });
  }
});

// Create a new todo
app.route("/api/v1/todos").post(async (req, res) => {
  const todoTitle = req.body.title;
  console.log("CALLED POST api/v1/todos with title=" + todoTitle);

  try {
    // Insert todo in postgres DB
    await postgresClient.query("INSERT INTO todo(title) VALUES($1)", [
      todoTitle,
    ]);
    console.log("  Added Todo: [" + todoTitle + "] to Database");

    // Update the Redis cache (add the todo text to the Set)
    await redisClient.sAdd("todos", todoTitle);
    console.log("  Added Todo: [" + todoTitle + "] to Cache");

    // Update the search index (ES 8.x - no type parameter)
    await elasticClient.index({
      index: TODO_SEARCH_INDEX_NAME,
      document: { todotext: todoTitle },
    });
    console.log("  Added Todo: [" + todoTitle + "] to Search Index");

    res.status(201).send(req.body);
  } catch (error) {
    console.log("Error creating todo: " + error);
    res.status(500).send({ error: "Failed to create todo" });
  }
});

// Search all todos
app.route("/api/v1/search").post(async (req, res) => {
  const searchText = req.body.searchText;
  console.log("CALLED POST api/v1/search with searchText=" + searchText);

  try {
    const results = await elasticClient.search({
      index: TODO_SEARCH_INDEX_NAME,
      query: {
        match: {
          todotext: searchText,
        },
      },
    });
    console.log('Search for "' + searchText + '" matched: ', results.hits.hits);
    res.send(results.hits.hits);
  } catch (error) {
    console.log("Search error: " + error);
    res.send([]);
  }
});

// Start the server /////////////////////////////////////////////////////
app.listen(port, () => {
  console.log("Todo API Server started!");
});
