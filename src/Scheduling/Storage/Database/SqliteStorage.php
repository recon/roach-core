<?php

namespace RoachPHP\Scheduling\Storage\Database;

use PDO;
use RoachPHP\Http\Request;

class SqliteStorage implements DatabaseStorageInterface
{
    private const SQL_STATE_UNIQUE_INDEX_VIOLATION = 23000;

    protected string $dbNamespace;

    protected ?PDO $sqliteConnection = null;

    public function setNamespace($namespace)
    {
        $this->dbNamespace = $namespace;
    }

    public function pushItem(Request $request, string $path)
    {
        $connection = $this->getPdoConnection();
        $statement = $connection->prepare("INSERT INTO queue (request_payload, path) VALUES (?, ?)");

        try {
            $statement->execute([serialize($request), $request->getPath()]);
        } catch (\PDOException $e) {
            if (!in_array($e->getCode(), [
                static::SQL_STATE_UNIQUE_INDEX_VIOLATION
            ])) {
                throw $e;
            }
        }
    }

    public function pullItems(int $batchSize): array
    {
        $connection = $this->getPdoConnection();
        $statement = $connection->prepare("SELECT rowid, request_payload FROM queue WHERE is_fetched = false LIMIT ?");
        $statement->execute([$batchSize]);

        $data = $statement->fetchAll();
        $result = [];

        $callback = function (&$input) use (&$result) {
            $result[] = unserialize($input['request_payload']);
        };
        @array_walk($data, $callback);

        $statement = $connection->prepare("UPDATE queue SET is_fetched=true WHERE rowid IN (?)");
        $statement->execute([implode(',', array_column($data, 'rowid'))]);

        return $result;
    }

    public function isEmpty(): bool
    {
        $connection = $this->getPdoConnection();
        $statement = $connection->query("SELECT rowid FROM queue WHERE is_fetched = false LIMIT 1");
        $result = $statement->fetch();

        return empty($result['rowid']);
    }

    protected function getPdoConnection(): PDO
    {
        if (is_null($this->sqliteConnection)) {
            $this->initializeConnection();
        }

        return $this->sqliteConnection;
    }

    protected function initializeConnection()
    {
        $dsn = sprintf('sqlite:./storage/%s.sqlite3', $this->dbNamespace);
        $this->sqliteConnection = new PDO($dsn);
        $this->sqliteConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->sqliteConnection->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $statement = $this->sqliteConnection->exec("
                CREATE TABLE IF NOT EXISTS queue (
                    request_payload TEXT NOT NULL,
                    path TEXT NOT NULL,
                    is_fetched BOOLEAN DEFAULT false NOT NULL, 
                    date_insert DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE UNIQUE INDEX IF NOT EXISTS IX_queue_path ON queue (path);
            ");
    }
}