<?php

declare(strict_types=1);

/**
 * Copyright (c) 2023 Kai Sassnowski
 *
 * For the full copyright and license information, please view
 * the LICENSE file that was distributed with this source code.
 *
 * @see https://github.com/roach-php/roach
 */

namespace RoachPHP\Scheduling;

use DateInterval;
use DateTimeImmutable;
use RoachPHP\Http\Request;
use RoachPHP\Scheduling\Timing\ClockInterface;
use PDO;

final class SqliteRequestScheduler implements RequestSchedulerInterface
{
    private const SQL_STATE_UNIQUE_INDEX_VIOLATION = 23000;

    private int $delay = 0;

    /**
     * @var array<Request>
     */
    private array $requests = [];

    private DateTimeImmutable $nextBatchReadyAt;

    private $dbNamespace = '';

    private $sqliteConnection = null;

    public function __construct(private ClockInterface $clock)
    {
        $this->nextBatchReadyAt = $this->clock->now();
    }

    public function schedule(Request $request): void
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

        $this->requests[] = $request;
    }

    public function empty(): bool
    {
        $connection = $this->getPdoConnection();
        $statement = $connection->query("SELECT rowid FROM queue WHERE is_fetched = false LIMIT 1");
        $result = $statement->fetch();

        return empty($result['rowid']);
    }

    /**
     * @return array<Request>
     */
    public function nextRequests(int $batchSize): array
    {
        $this->clock->sleepUntil($this->nextBatchReadyAt);

        $this->updateNextBatchTime();

        return $this->getNextRequests($batchSize);
    }

    public function forceNextRequests(int $batchSize): array
    {
        return $this->getNextRequests($batchSize);
    }

    public function setDelay(int $delay): RequestSchedulerInterface
    {
        $this->delay = $delay;

        return $this;
    }

    private function getNextRequests(int $batchSize): array
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

        $statement = $connection->prepare("UPDATE queue SET is_fetched=true WHERE rowid = ?");
        $statement->execute([implode(',', array_column($data, 'rowid'))]);

        return $result;
    }

    public function setNamespace(string $namespace): RequestSchedulerInterface
    {
        $this->dbNamespace = strtolower(preg_replace('/[^a-z0-9]/i', '', $namespace));

        return $this;
    }

    private function updateNextBatchTime(): void
    {
        $this->nextBatchReadyAt = $this->clock->now()->add(new DateInterval("PT{$this->delay}S"));
    }

    /**
     * @return array<Request>
     */

    protected function getPdoConnection(): PDO
    {
        if (is_null($this->sqliteConnection)) {
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

        return $this->sqliteConnection;
    }

}
