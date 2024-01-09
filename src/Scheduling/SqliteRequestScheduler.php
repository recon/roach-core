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
use RoachPHP\Scheduling\Storage\Database\DatabaseStorageInterface;
use RoachPHP\Scheduling\Timing\ClockInterface;
use PDO;

final class SqliteRequestScheduler implements RequestSchedulerInterface
{

    private int $delay = 0;

    private DateTimeImmutable $nextBatchReadyAt;

    private $dbNamespace = '';

    private $sqliteConnection = null;

    public function __construct(private ClockInterface $clock, private DatabaseStorageInterface $databaseAdapter)
    {
        $this->nextBatchReadyAt = $this->clock->now();
    }

    public function schedule(Request $request): void
    {
        $this->databaseAdapter->pushItem($request, $request->getPath());
    }

    public function empty(): bool
    {
        return $this->databaseAdapter->isEmpty();
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
        return $this->databaseAdapter->pullItems($batchSize);
    }

    public function setNamespace(string $namespace): RequestSchedulerInterface
    {
        $this->databaseAdapter->setNamespace(strtolower(preg_replace('/[^a-z0-9]/i', '', $namespace)));

        return $this;
    }

    private function updateNextBatchTime(): void
    {
        $this->nextBatchReadyAt = $this->clock->now()->add(new DateInterval("PT{$this->delay}S"));
    }

}
